## Java version used:
    Corretto-1.8 (Amazon) 

## Language Level
    SDK default (8- Lambdas, type annotations etc.)

## Spark Versions:
    
    Spark: 351
    Java_spark_package: 2.12


## Implementata MRPrintStatistics:: 
  la funzione prende in input due RDD che contengono 
 le collezioni dei punti. Il primo e' l'universo dei punti e il secondo l'RDD dei centri.
 Il tipo dei punti e' generico cosi' da supportare punti con diverse dimensioni.
 La funzione printa le statistiche nel seguente formato:
```
    i = 0, center = (40.749035,-73.984431), NA0 = 725, NB0 = 192
    i = 1, center = (40.873440,-74.192170), NA1 = 7, NB1 = 3
    i = 2, center = (40.693363,-74.178147), NA2 = 19, NB2 = 11
    i = 3, center = (40.746095,-73.830627), NA3 = 31, NB3 = 24
```
Uniche pecce:
```
 -  utilizzo di contatore atomico per contare l'indice degli elementi
 -  collect globale di tutto l'RDD in un nodo. Funziona perche' siamo in globale,
    in cluster si spacca
```

## MRComputeStandardObjective

Calcola la distanza media quadratica per ogni punto in P dai centri S.

Round 1:
- MapPhase: Crea per ogni punto una tripla con (indice_partizione, distanza_quadratica, 1 (per conteggio))
- ReducePhase: Raggruppa per ogni partizione e calcola la somma, in output ottengo per ogni partizione (1, (somma_distanze_quadratiche_part, numero_punti_part))
Round 2:
- MapPhase: Raggruppo tutti i conteggi parziali (chiave 1), ottenendo in uscita (somma_distanze_quadratiche, totale_punti)

Alla fine viene restituita la divisione tra somma_distanze_quadratiche e totale_punti

## MRComputeFairObjective

Calcola la funzione obiettivo "fair".
Filtra l'insieme in ingresso per i punti della classe 'A' e 'B' e poi invoca MRComputeStandardObjective su i due RDD.
Viene restituito il massimo tra i due risultati.