import sys
import threading
import time
import random
import Pyro5.server
import Pyro5.api
from Pyro5.core import locate_ns
from Pyro5.server import expose

PREFIXO_URI = "PYRO:Object"

def main(object_id):
    try:
        name = f"Object{object_id}"
        print(f"Starting node: {name}")
        name_list = [f"Object{i}" for i in range(1,5)]

        uris = [f"{PREFIXO_URI}{i}@localhost:{7000 + i}" for i in range(1, 5)]
        RaftNode(name, object_id, uris)
    except Pyro5.errors.CommunicationError as ce:
        print("Communication error:", ce)
    except Pyro5.errors.NamingError as ne:
        print("Error locating the Name Server:", ne)
    except Exception as e:
        print("An unexpected error occurred:", e)

@expose
class RaftNode(object):
    def __init__(self, node_name: str, node_id: int, uris):
        self.role = 'follower'
        self.name = node_name
        self.id = node_id
        self.term = 0
        self.arr = []
        self.uris = uris
        self.last_heartbeat = 0
        self.value = 0
        self.uncommitted_value = 0
        self.running = True

        # Inicializa o servidor Pyro
        port = 7000 + node_id
        daemon = Pyro5.server.Daemon(port=port)
        self.uri = daemon.register(self, node_name)
        print(f"Node ID: {self.id}")
        print(f"Node URI: {self.uri}")
        
        # Inicia uma nova thread para o loop principal
        threading.Thread(target=self.run).start()
        
        # Inicia o loop do servidor Pyro
        daemon.requestLoop()


    def run(self):
        time.sleep(random.randint(5, 10))
        while self.running:
            sleep_time = random.randint(1000, 5000) / 1000
            self.last_heartbeat = sleep_time
            time.sleep(sleep_time)
            print(f'Node arr: {self.arr}')
            # Verifica se é um seguidor e se houve um último heartbeat
            if self.role == 'follower' and self.last_heartbeat != 0:
                self.start_election()

    
    def send_heartbeat(self):
        self.running = True
        
        while self.running:
            # Lista de URIs excluindo o próprio URI
            uris_to_send = [uri for uri in self.uris if uri != self.uri]
            # Envia heartbeat para cada URI
            for uri in uris_to_send:
                try:
                    proxy = Pyro5.api.Proxy(uri)
                    proxy.receive_heartbeat(self.term, self.arr, self.uri)
                except Pyro5.errors.CommunicationError:
                    print(f"Node {self.id} failed to send heartbeat to {uri}")
                except Pyro5.errors.SerializeError:
                    print(f"Node {self.id} encountered serialization error while sending heartbeat to {uri}")
                    
            time.sleep(0.05)

    def receive_heartbeat(self, term, arr, uri):
        if int(self.term) == int(term) - 1:
            self.term = term
            self.arr = arr
            self.last_heartbeat = 0
            return
        elif term == self.term:
            self.last_heartbeat = 0

    def start_election(self):
        print(f"Node {self.id} started an election")
        self.role = 'candidate'
        votes = 0
        connections = len(self.uris)

        for uri in self.uris:
            if uri == self.uri:
                votes += 1
                continue
            try:
                votes += self.request_vote(uri)
            except:
                connections -= 1

        # Eleito
        if votes > connections // 2 and self.role == 'candidate':
            print(f"Node {self.id} é um líder")
            self.role = 'leader'
            self.term += 1
            threading.Thread(target=self.send_heartbeat).start()
            ns = locate_ns()
            ns.register('leader', self.uri)
        else:
            print(f"Eleição do Node: {self.id} falhou. ")
            self.role = 'follower'

    def request_vote(self, uri):
        print(f"Node {self.id} pedindo votos para: {uri}")
        proxy = Pyro5.api.Proxy(uri)
        got_vote = proxy.vote(self.term, self.uri)
        if got_vote:
            print(f"Node {self.id} votou do seguinte URI: {uri}")
            return 1
        return 0

    def vote(self, candidate_term, candidate_uri):
        print(f"Node {self.id} pedindo votos do URI: {candidate_uri}")
        if candidate_term != self.term:
            print(
                f"Node {self.id} não votou, o term ({self.term}) é um candidato diferente do term: ({candidate_term})")
            return False
        if self.role != 'follower':
            print(f"Node {self.id} não votou porque é um: {self.role}")
            return False
        print(f"Node {self.id} votou para: {candidate_uri}")
        return True

    def set_leader_value(self, value):
        self.uncommitted_value = value
        
        if self.role != 'leader':
            print('Não comittou, não é o líder')
            return
        
        commits = 0
        total_connections = len(self.uris)
        
        for uri in self.uris:
            # Ignorar a conexão para si mesmo
            if self.uri == uri:
                commits += 1
                continue
            try:
                proxy = Pyro5.api.Proxy(uri)
                proxy.set_value(value)
                commits += 1
            except Pyro5.errors.CommunicationError:
                # Tratamento específico para falhas de comunicação
                total_connections -= 1
                print(f"Node {self.id} NÃO setou o valor: {value} na URI {uri}")
            except Pyro5.errors.SerializeError:
                # Tratamento específico para erros de serialização
                total_connections -= 1
                print(f"Node {self.id} erro de serialization na URI {uri}")
        
        # Verificar se a maioria dos nós aceitou o valor
        if commits > total_connections // 2:
            self.commit_values()
        else:
            print(f"Não recebeu todos os commits")


    def set_value(self, value):
        print(f"Node {self.id} setando o valor não commitado: {value}")
        self.uncommitted_value = value

    def commit_values(self):
        for uri in self.uris:
            try:
                # Tenta enviar o valor para o nó remoto
                proxy = Pyro5.api.Proxy(uri)
                proxy.append_value(self.id, self.term)
                print(f'Commitadoo valor para o URI: {uri}')
            except Pyro5.errors.CommunicationError:
                # Trata erros de comunicação específicos
                print(f'Node {self.id} falhou ao commitar valor para o URI: {uri}: Communication error')
            except Pyro5.errors.SerializeError:
                # Trata erros de serialização específicos
                print(f'Node {self.id} falhou ao commitar valor para o URI: {uri}: Serialization error')
            except Exception as e:
                # Trata outros erros de forma genérica
                print(f'Node {self.id} falhou ao commitar valor para o URI: {uri}: {str(e)}')


    def append_value(self, leader_id, leader_term):
        try:
            # Verifica se o líder e o termo fornecidos são válidos
            if leader_id is None or leader_term is None:
                print("leader_id ou leader_term INVALIDO")
                return
            
            print(f"Node {self.id} commitando valor: ' {self.uncommitted_value} '")
            
            # Cria um objeto com o valor, líder e termo
            obj = {
                'value': self.uncommitted_value,
                'leader': leader_id,
                'term': leader_term
            }            
            # Atualiza o valor atual e adiciona ao arr
            self.value = self.uncommitted_value
            self.arr.append(obj)
            
            # Confirmação da operação
            print("Valor Commitado.")
        except Exception as e:
            # Tratamento genérico de exceções
            print(f"Erro append_value: {str(e)}")


if __name__ == "__main__":
    object_id = int(sys.argv[1])
    main(object_id)
