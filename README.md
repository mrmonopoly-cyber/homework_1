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
- MapPhase: Per ogni punto dell'universo cerca il centro piu' vicino e poi crea una tupla che indica a che gruppo (A,B) 
  appartiene
- ReducePahse: Raggruppa tutto e somma gli i valori delle tuple tra di loro per ogni centro ottenendo alla fine una lista

Alla fine printa su terminale il risulatato voluto scandendo la lista.
L'ordine nel print e' garantito da un'operazione di sorting fatta prima della costruzione della
lista sulla chiave (indice dei centri)


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