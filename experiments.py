import node1, node2, node3
import threading

t1 = threading.Thread(target=node1.run)
t2 = threading.Thread(target=node2.run)
t3 = threading.Thread(target=node3.run)

t1.start()
t2.start()
t3.start()