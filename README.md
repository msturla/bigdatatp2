bigdatatp2
==========
Para compilar, correr ./compile.sh
Esto compilara el .jar si esta maven instalado, caso contrario se puede usar el jar en /target

Para crear las tablas, usar ./make.sh
Para tirar los datos y recrear las tablas, usar ./update.sh

Para correr el tp, usar:

storm jar bigdatatp2-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.globant.itba.storm.bigdatatp2.MainTopology --topologyname=top --msgqueuename=name

en donde top es el nombre de la topologia y name es el nombre de la cola.
si no se especifica el nombre de la topologia se usara el default: test
si no se especifica el nombre de la cola se usara el default:name
