import xmlrpc.client
import time

proxy = xmlrpc.client.ServerProxy('http://localhost:9000')

aux = 7
print('WORKER 1 CREAT')
print(proxy.createWorker())
while (aux != -1):
    print('\n1. Create Worker')
    print('2. List Workers')
    print('3. Delete Worker')
    print('4. Word Count')
    print('5. Counting Words')
    print('6. EXIT\n')
    num = int(input("Escull una opció: "))

    aux = 0
    if (num == 1): aux = proxy.createWorker()
    if (num == 2): aux = proxy.listWorker()
    if (num == 3): 
        quin = int(input("ELIMINAR ID worker:"))
        aux = proxy.deleteWorker(quin)
    if ((num == 4) or (num == 5)):
        fitxers = int(input("NOMBRE FITXERS A TRACTAR? "))
        i = 0
        fitxer = ''
        while ((i < fitxers) and (fitxers != 0)):
            nom_fitxer = input("Nom file: ")
            if ((fitxers > 1) and (i < fitxers-1)):
                fitxer = str(fitxer) + str(nom_fitxer) + '*'
            if (i == fitxers-1):
                fitxer = str(fitxer) + str(nom_fitxer)
            i = i + 1
        if (num == 4):
            tasca = 'wordCount'
        else: tasca = 'countingWords'

        job_id = proxy.tractar_cua(tasca, fitxer)
        time.sleep(5)
        print('\nRESULTAT '+ tasca + ': ')
        aux = proxy.getResult(str(job_id), tasca)

    if (num == 6): aux = -1

    print(aux)
