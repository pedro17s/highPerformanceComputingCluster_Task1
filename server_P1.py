import logging
import os
import redis
import time
import sys
from redis.client import Redis
from xmlrpc.server import SimpleXMLRPCServer
from multiprocessing import Process

WORKERS = {}
WORKER_ID = 0
llista = {}
cua = 'cua_jobs' 
r = None


def startWorker(w_id):
    global cua
    global r
    
    while True:
        elem = r.lpop(cua)

        if elem != None:
            line = str(elem).split(':')
            task = line[0]
            task = task[2:]
        
            fitxer = line[1]
            fitxer = fitxer[:len(fitxer)]
            
            cont = line[2]
            cont = cont[2:len(cont)-1]

            num = line[3]
            num =  num[:len(num)-1]

            url = 'http://localhost:8000/' + fitxer
            os.system('curl ' + url + '> redireccio_'+ cont + num + '.txt')
            arg1 = 'redireccio_' + cont + num + '.txt'
            
            result = eval(task)(arg1)
            if (int(num) > 0):
                time.sleep(1)

                aux = r.get(cont)
                aux = str(aux)
                aux = aux[2:len(aux)-1]  
                
                result = aux + '*' + str(result)

            r.mset({cont : str(result)})
            os.system('rm ' + arg1)

    return True


# Función que le pasamos por parámetro el job que tenga que hacer y ficheros.Organiza el job y lo añade a la cola de Redis.
def tractar_cua(task, files):
    global cua
    global r

    i=0
    fitxers = files.split('*')
    if (files != ''):
        while (i < len(fitxers)):
            if (i < 1):
                r.incr("counter")
                r.set(r.get("counter"), 0)
            arg = str(task) + ':' + str(fitxers[i]) + ':' + str(r.get("counter")) + ':' + str(i)
            r.rpush(cua, arg)
            i = i + 1
        return (r.get("counter"))
    else: 
        return 0

# Creamos un proceso y llamamos a la función startWorker().
def createWorker():
    global WORKERS
    global WORKER_ID

    proces = Process(target=startWorker, args=(WORKER_ID,))
    proces.start()
    
    WORKERS[WORKER_ID] = proces
    WORKER_ID =  WORKER_ID+1
    return ('WORKER CREAT = ',WORKER_ID)


# Función que elimina el worker con el identificador que se escoja. 
def deleteWorker(cont):
    global WORKERS
    global WORKER_ID
    
    proces = WORKERS[cont-1]
    proces.terminate()
    proces.is_alive()
    WORKERS.pop(cont-1)
    
    return ('WORKER ELIMINAT= ',cont)


# Función que lista los workers creados.
def listWorker():
    global WORKERS
    global WORKER_ID

    llista1 = {}
    for i in WORKERS:
        prova = str(WORKERS[i])
        prova1 = prova.find(",")
        llista1[i] = '<(WORKER= {}, {}'.format(i+1, str(prova[prova1+1:]))

    return ('LLISTA DE WORKERS= ',str(llista1))


# Funció que cuenta el numero de palabras que hay en un fichero.
def countingWords(fitxer):
    f = open(fitxer, "r")
    line = f.read()
    num = len(line.split())

    return (num)


# Función que cuenta la frecuencia de cada palabra del texto pasado por parámetro.
def wordCount(fitxer):
    f = open(fitxer, "r")
    line = f.read()
    line = line.lower()
    
    counts = dict()
    words = line.split()

    for word in words:
        if word in counts:
            counts[word] = counts[word] + 1
        else:
            counts[word] = 1
    
    return (counts)


# Llamamos a la función wordCount para tratar las palabras repetidas .
def tractar_llista(llista):
    auxiliar = []
    i = 0
    boolea = False
    
    while (i < len(llista)):
        aux = llista[i]
        key = aux[0]
        valor = aux[1]
        y = 0
        boolea = False
        while ((y < len(auxiliar)) and boolea == False):
            if (key == auxiliar[y][0]):
                boolea = True
            y = y + 1

        if (boolea == False):
            auxiliar.append(aux)
            
        else:
            aux1 = auxiliar[y-1]
            valor1 = aux1[1]
            valor1 = int(valor1) + int(valor)
            auxiliar[y-1][1] = valor1

        i = i + 1
    
    return auxiliar

    
# Obtenemos el resultado de la tarea.
def getResult(job_id, task):
    global r

    llista = []
    if (job_id != '0'):
        result = r.get(job_id)
        r.delete(job_id)
        result = str(result)
        result = result[2:len(result)-1]
        i = 0
        suma = 0

        if (result.find("*") > 0):
            if (task == 'countingWords'):
                line = result.split('*')

                while (i < len(line)):
                    suma = int(line[i]) + suma
                    i = i + 1
                result = suma

            elif (task == 'wordCount'):
                result =  result[1:len(result)-1]            
                result = result.replace('}*{', ', ')

                line = result.split(',')
                i = 0
                while (i < len(line)):
                    line1 = str(line[i])
                    aux = line1.split(':')
                    key = aux[0]

                    if (i == 0):
                        key = key[1:len(key)-1]
                    else:
                        key = key[2:len(key)-1]

                    value = aux[1]
                    value =  value[1:]

                    llista.append([key, value])
                    i = i + 1
            
                i = 0
                result = tractar_llista(llista)
        return (result)
    else:
        result = '0 fitxers'
        return (result)

 #MAIN
if __name__ == '__main__':
        
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    server = SimpleXMLRPCServer(
       ('localhost', 9000),
        logRequests=True,
       )

    server.register_function(createWorker)
    server.register_function(deleteWorker)
    server.register_function(listWorker)
    server.register_function(countingWords)
    server.register_function(wordCount)
    server.register_function(tractar_cua)
    server.register_function(getResult)
    
    r = redis.Redis()
    r.set("counter", 0)

    # Start the server
    try:
        print('Control+C PER SURTIR')
        server.serve_forever()
    except KeyboardInterrupt:
        print('Exiting')
