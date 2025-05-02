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


## Assignment (2) specification
Assignment of Homework 2 (deadline: May 15)
The purpose of the second homework is to implement the variant of Lloyd's algorithm for fair k-means clustering, proposed in the paper Socially Fair k-Means Clustering (ACM FAccT'21), and to compare its effectiveness against the standard variant, with respect to the new objective function introduced in that paper. Moreover, you will test the scalability of your implementation on the CloudVeneto cluster available for the course. To get access and to use the cluster you must strictly follow the rules indicated in the  User guide for the CloudVeneto cluster which you can find in the same section as this specification.  For this homework, you will be able to recycle most of the work done for Homework 1: make sure to correct bugs (if any) and to include the feedback we will give you on Homework 1.

FAIR K-MEANS CLUSTERING. As in Homework 1, we consider a set of points U⊂Rd
,  split into two demographic groups A
,B
, that is, U=A∪B
. Given a number of clusters k
, the goal is to find a set C
 of centroids which minimize the following objective function

Φ(A,B,C)=max{(1/|A|)∑a∈A(dist(a,C))2,(1/|B|)∑b∈B(dist(b,C))2}
  

To this purpose, you will implement the following variant of Lloyd's algorithm. Let M
 be a user-defined integer parameter.

Compute an initial set {c1,c2,…,ck}
 of k
 centroids
Repeat M times:
2.1. Partition U
 into k
 clusters U1,U2,⋯,Uk
, where Ui
 consists of the points of U
 whose closest current centroid is ci
  (assume that ties are broken in favor of the smallest index).
2.2. Compute a new set {c1,c2,…,ck}
 of k
 centroids using the CentroidsSelection algorithm described here.
INPUT FORMAT and REPRESENTATION OF POINTS. For both issues please follow what done in Homework 1, correcting bugs, if any.

ASSIGNMENT for HW2. You must do the following tasks.

1) Write a method/function MRFairLloyd which implements the above Fair K-Means Clustering algorithm. Specifically, MRFairLloyd takes in input an RDD representing a set U
 of points, with demographic group labels, and two parameters  K,M,
 (integers), and does the following:

Initializes a set C
 of K
 centroids using kmeans|| (this can be achieved by running the Spark implementation of LLody's algorithm with 0 iterations).
Executes M
 iterations of the above repeat-until loop.
Returns the final set C
 of centroids.
The set C
 must be represented as an array of Vector in Java and an array of tuples in Python.

2) Include method/function MRComputeFairObjective from HW1, which takes in input the set U=A∪B
 and a set C
 of centroids, and returns the value of the objective function Φ(A,B,C)
 described above. Make sure to correct bugs, if any.

3) Write a program GxxHW2.java (for Java users) or GxxHW2.py (for Python users), where xx is your 2-digit group number, which receives in input, as command-line arguments, a path to the file storing the input points, and 3 integers L,K,M
, and does the following:

Prints the command-line arguments and stores  L,K,M
 into suitable variables.
Reads the input points into an RDD -called inputPoints-, subdivided into L
 partitions.
Prints the number N
 of points, the number NA
 of points of group A, and the number NB
 of points of group B (hence, N=NA+NB
). 
Computes a set Cstand
 of K
 centroids for the input points, by running the Spark implementation of the standard Lloyd's algorithm,  with M
 iterations, disregarding the demographic groups.
Computes a set Cfair
 of K
 centroids by running MRFairLloyd(inputPoints,K,M).
Computes and prints Φ(A,B,Cstand)
 and Φ(A,B,Cfair)
Prints separately the times, in seconds, spent to compute : Cstand
, Cfair
, Φ(A,B,Cstand)
 and Φ(A,B,Cfair)
.
RUNNING TIMES: To take running times correctly, read carefully Section "Profiling" in our guide:  Introduction to Programming in Spark

OUTPUT FORMAT: In the same section as this page you will find some datasets and outputs corresponding to specific input configurations using these datasets. Your program must STRICTLY ADHERE TO THE OUTPUT FORMAT used in these examples (TO BE ADDED). Any deviation from this format will be penalized.

4) Test your program as follows:

Test and debug the program in local mode on your PC to make sure that it runs correctly. 
Only after you are confident that your program runs correctly, run it on the cluster using the datasets which have been preloaded in the HDFS, and fill the table given in this word file (TO BE ADDED) with the results of the specified experiments.
WHEN USING THE CLUSTER, YOU MUST STRICTLY FOLLOW THESE RULES:

To avoid congestion, groups with even (resp., odd) group number must use the clusters in even (resp., odd) days.
Do not run several instances of your program at once.
Do not use more than 16 executors.
Try your program on a smaller dataset first.
Remember that if your program is stuck for more than 1 hour, its execution will be automatically stopped by the system.
5) Write a program GxxGEN.java (for Java users) or GxxGEN.py (for Python users), where xx is your 2-digit group number, which receives in input, as command-line arguments, 2 integers N,K
, and generates a dataset of N
 points in \(\mathbb{R}^2\), which, if used as input for the above program, show a clear quality gap between the solutions provided by the standard LL'oyd's algorithm and its fair variant. The program must print the points and their respective demographic groups in output, using the same format as for the input points. A short description of the generator must be given in the word file used to report the experiments on the cluster.
SUBMISSION INSTRUCTIONS. Each group must submit a zipped folder GxxHW2.zip, where xx is your group number. The folder must contain the programs (GxxHW2.java/GxxGEN.java or GxxHW2.py/GxxGEN.py) and a file GxxHW2file.docx with the aforementioned table and the description of the generator. Only one student per group must do the submission using the link provided in the Homework 2  section. Make sure that your code is free from compiling/run-time errors and that you comply with the specification, otherwise your grade will be penalized.

If you have questions about the assignment, contact the teaching assistants (TAs) by email to bdc-course@dei.unipd.it . The subject of the email must be "HW2 - Group xx", where xx is your group ID. If needed, a zoom meeting between the TAs and the group will be organized.
