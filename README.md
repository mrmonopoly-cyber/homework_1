## Java version used:
    Corretto-1.8 (Amazon) 

## Language Level
    SDK default (8- Lambdas, type annotations etc.)

## Spark Versions:
    
    Spark: 351
    Java_spark_package: 2.12


## Implementata MRPrintStatistics:: 
Printa sul terminale i dati relativi al numero di punti di ogni insieme per ogni centro.
Round 1:
- Map Phase: Partizionamento dell'insieme unioverso di input. In ogni partizione cerco per ogni 
    punto il centro piu' vicino 
- Reduce Phase: Raggruppa i valori delle partizioni per ogni chiave(indice del centro) ottengo (indice del centro, Na, Nb)
 Le triple sono poi ordinate basandosi sulla chiave (indice del centro)
Round 2:
- Reduce Phase: Scandisco linearmente la lista dei dati e stampo sul terminale 
 i dati nel seguente formato:
  i = [indice_del_centro], center = ([coordinate_del_centro]), NA = [numero_di_punti_in_A], NB = [numero_di_punti_in_B] 


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