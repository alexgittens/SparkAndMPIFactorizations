
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MULTIPLE_NODES

#ifdef MULTIPLE_NODES
#include <mpi.h>
#define MPI_BARRIER(nodeid) MPI_Barrier(MPI_COMM_WORLD);
#endif

#define TBD_MAX(alpha, beta) ((alpha) > (beta)) ? (alpha) : (beta)
#define CORE_FREQUENCY (3.5*1000.0*1000.0*1000.0)
extern "C" unsigned long long int read_tsc();

int nthreads;
int nnodes = -95123;
int node_id;


#include "color.h"

#define WARN_PRINT() {printf("Warning on line (%d) \n", __LINE__); }
#define ERROR_PRINT() {printf("Error on line (%d) \n", __LINE__); exit(123);}
#define ERROR_PRINT_STRING(abc) { printf("Error (%s)  on line (%d) in file (%s)\n", abc, __LINE__, __FILE__); exit(123); }
#define SUCCESS_PRINT() {PRINT_GREEN; printf("Everything Successful  on line (%d) \n", __LINE__); PRINT_BLACK;}

#define MAX_LINE_LENGTH (1000 * 1000)


unsigned long long int global_compute_atab_time = 0;
unsigned long long int global_compute_atab_part1_time = 0;
unsigned long long int global_compute_atab_part2_time = 0;
unsigned long long int *global_compute_atab_part1_per_thread_time = 0;
unsigned long long int *global_compute_atab_part2_per_thread_time = 0;


//+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
#include "parallel.h"
Parallel_Infrastructure global_parallel;

//+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
#include "memory.h"
Memory global_memory;
//+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
char global_input_filename[256];
char global_output_filename[256];
int *global_Starting_Row_Index;
int *global_Ending_Row_Index;

typedef struct SPARSE_MATRI
{
    int is_sparse;
    int number_of_rows, number_of_columns;
    long long int number_of_nnzs;
    int *Row;
    int *Column;
    float *Value;
    size_t *RowPtr;
    int mynode_number_of_rows;
    int mynode_starting_row_index; //Inclusive...
    int mynode_ending_row_index; //Not inclusive
    int mynode_number_of_nnzs;
}SPARSE_MATRIX;

SPARSE_MATRIX global_A, global_B, global_ATAB, global_ATAB_Reduced_Over_All_Nodes;
SPARSE_MATRIX **global_ATAB_Per_Thread;

//int k_prime = 32;
#define k_prime 32

void my_fgets(char *s, int size, FILE *stream)
{
    char *t = fgets(s, size, stream);
    if (t == NULL) ERROR_PRINT();
}


void Fill_Up_RowPtrs(SPARSE_MATRIX *A)
{
    int number_of_rows = A->mynode_number_of_rows;

    size_t *RowPtr = A->RowPtr;

    int mynode_starting_row_index = A->mynode_starting_row_index;
    //printf("[%d] -- %d\n", node_id, mynode_starting_row_index);

    RowPtr[mynode_starting_row_index - mynode_starting_row_index] = 0;

    int prev_row = mynode_starting_row_index;

    for(size_t k = 0; k < A->mynode_number_of_nnzs; k++)
    {
        if (A->Row[k] != prev_row)
        {
            //printf("prev_row = %d ::: mynode_starting_row_index = %d ::: k = %d\n", prev_row, mynode_starting_row_index, k);
            RowPtr[(prev_row - mynode_starting_row_index) + 1] = k;

            for(int q = prev_row + 1; q <= A->Row[k]; q++)
            {
                RowPtr[q - mynode_starting_row_index] = k;
            }

            prev_row = A->Row[k];
        }
    }

    for(int q = prev_row + 1; q <= A->mynode_ending_row_index; q++)
    {
        RowPtr[q - mynode_starting_row_index] = A->mynode_number_of_nnzs;
    }
}
        
void Compute_Per_Node_Data(SPARSE_MATRIX *A)
{
    //ASSUMPTION: Dividing rows evenly between the nnodes nodes...

    int number_of_nnzs = A->mynode_number_of_nnzs;
    int number_of_rows = A->number_of_rows;

    printf("number_of_rows = %d\n", number_of_rows);
    int number_of_rows_per_node = (number_of_rows % nnodes) ? (number_of_rows/nnodes + 1) : (number_of_rows/nnodes); 
    int starting_row_index = (node_id + 0) * number_of_rows_per_node;
    int   ending_row_index = (node_id + 1) * number_of_rows_per_node;

    if (starting_row_index > number_of_rows) starting_row_index = number_of_rows;
    if (  ending_row_index > number_of_rows)   ending_row_index = number_of_rows;

    A->mynode_starting_row_index = starting_row_index;
    A->mynode_ending_row_index   = ending_row_index;
    A->mynode_number_of_rows = A->mynode_ending_row_index - A->mynode_starting_row_index;

    //This variable is not known from the beginning... We will approximate it, and allocate double of that...
    A->mynode_number_of_nnzs = (A->number_of_nnzs/nnodes) * 1.1;

    printf("mynode_starting_row_index = %d ::: mynode_ending_row_index = %d ::: mynode_number_of_rows = %d\n", A->mynode_starting_row_index, A->mynode_ending_row_index, A->mynode_number_of_rows);
}

void Parse_File_MTX(char *filename, SPARSE_MATRIX *A)
{
    //Each node is calling it... Keep that in mind...

    unsigned long long int stime = read_tsc();

    char *line = (char *)global_memory.small_malloc(MAX_LINE_LENGTH);

    int is_ascii = 0;
    int is_binary = 0;

    FILE *fp = fopen(filename, "r");
    if (fp == NULL) { printf("File %s not found\n", filename); exit(123); }

    my_fgets(line, MAX_LINE_LENGTH, fp);

    if (!(strncmp(line, "%%MatrixMarket matrix coordinate real general", strlen("%%MatrixMarket matrix coordinate real general")))) 
    {
        is_ascii = 1;
        my_fgets(line, MAX_LINE_LENGTH, fp);
        sscanf(line, "%d %d %lld", &(A->number_of_rows), &(A->number_of_columns), &(A->number_of_nnzs));
        printf("nnzs = %d\n", A->number_of_nnzs);
        ERROR_PRINT_STRING("ASCII file are not supported in multi-node version...");
    }
    else
    {
        fseek(fp, 0, SEEK_SET);
        fread(line, strlen("%%MatrixMarket matrix coordinate real bin"), 1, fp);
        if (!(strncmp(line, "%%MatrixMarket matrix coordinate real bin", strlen("%%MatrixMarket matrix coordinate real bin")))) 
        {
            is_binary = 1;
            fread(&(A->number_of_rows), sizeof(int), 1, fp);
            fread(&(A->number_of_columns), sizeof(int), 1, fp);
            fread(&(A->number_of_nnzs), sizeof(long long int), 1, fp);
            printf("nnzs = %d\n", A->number_of_nnzs);
        }
        else
        {
            ERROR_PRINT();
        }

        Compute_Per_Node_Data(A);
    }


    size_t approximate_number_of_nnzs_for_malloc = A->mynode_number_of_nnzs * 1.2;
    printf("approximate_number_of_nnzs_for_malloc = %lld\n", approximate_number_of_nnzs_for_malloc);

    A->Row    = (int *)global_memory.small_malloc((size_t)(approximate_number_of_nnzs_for_malloc) * (size_t)(sizeof(int)));
    A->Column = (int *)global_memory.small_malloc((size_t)(approximate_number_of_nnzs_for_malloc) * (size_t)(sizeof(int)));
    A->Value  = (float  *)global_memory.small_malloc((size_t)(approximate_number_of_nnzs_for_malloc) * (size_t)(sizeof(float)));
    A->RowPtr = (size_t *)global_memory.small_malloc((size_t)(A->mynode_number_of_rows + 1) * (size_t)(sizeof(size_t)));

    A->is_sparse = 1;

    if (is_ascii == 1)
    {
        ERROR_PRINT_STRING("ASCII file are not supported in multi-node version...");
        for(long long int k = 0; k < A->number_of_nnzs; k++)
        {
            my_fgets(line, MAX_LINE_LENGTH, fp);
            sscanf(line, "%d %d %f", A->Row + k, A->Column + k, A->Value + k);
            A->Row[k]--;
            A->Column[k]--;
        }
    }
    else if (is_binary == 1)
    {
        if (A->number_of_nnzs % A->number_of_rows) ERROR_PRINT_STRING("A->number_of_nnzs % A->number_of_rows");
        size_t number_of_nnzs_per_row = A->number_of_nnzs/A->number_of_rows;
        size_t number_of_nnzs_before_mynode = A->mynode_starting_row_index * number_of_nnzs_per_row;

        A->mynode_number_of_nnzs = A->mynode_number_of_rows * number_of_nnzs_per_row;
        printf("node_id = %d ::: nnzs = %lld\n", node_id, A->mynode_number_of_nnzs);

        size_t size_to_be_skipped = number_of_nnzs_before_mynode  * (2 * sizeof(int) + sizeof(float));
        size_t size_to_be_read = A->mynode_number_of_nnzs * (2 * sizeof(int) + sizeof(float));
        unsigned char *XYZ = (unsigned char *)global_memory.small_malloc(size_to_be_read);

        fseek(fp, size_to_be_skipped, SEEK_CUR);
        fread(XYZ, size_to_be_read, 1, fp);

        for(long long int k = 0; k < A->mynode_number_of_nnzs; k++)
        {
            A->Row[k] =    *((int *)(XYZ + 12 * k + 0));
            A->Column[k] = *((int *)(XYZ + 12 * k + 4));
            A->Value[k] =  *((float *)(XYZ + 12 * k + 8));
            A->Row[k]--;
            A->Column[k]--;
            //if (node_id == 1) printf("k = %d ::: Row = %d :: Column = %d \n", k, A->Row[k], A->Column[k]);
        }
        //printf("......\n"); fflush(stdout);

        global_memory.small_free(XYZ, size_to_be_read);
    }
    else
    {
        ERROR_PRINT();
    }

    fclose(fp);

    unsigned long long int etime = read_tsc();
    unsigned long long int ttime = etime - stime;
    printf("Time Taken For Reading File  =  %lld cycles ( %.2lf seconds) \n", ttime, ttime/CORE_FREQUENCY);

    Fill_Up_RowPtrs(A);

    global_memory.small_free(line, MAX_LINE_LENGTH);
}
 
void Compute_Analytical_Formula(int k)
{
    if (node_id != 0) return;
    // 8 s k m n

    SPARSE_MATRIX *A = &global_A;

    double s = (A->number_of_nnzs * 1.0)/A->number_of_rows/A->number_of_columns;
    double total_flops = 1;

    total_flops *= 8;
    total_flops *= s;
    total_flops *= k;
    total_flops *= A->number_of_rows;
    total_flops *= A->number_of_columns;

    PRINT_GREEN
    printf("---------------------------------------------------------------\n");
    printf("s = %f\n", s);
    printf("Total Flops = %.2lf (%.2lf seconds)\n", total_flops, total_flops/CORE_FREQUENCY);
    printf("---------------------------------------------------------------\n");
    PRINT_BLACK
}

void Initialize_Dense_Matrix(SPARSE_MATRIX *A, int number_of_rows, int number_of_columns)
{
    A->is_sparse = 0;
    A->number_of_rows = number_of_rows;
    A->number_of_columns = number_of_columns;
    A->number_of_nnzs = (size_t)(number_of_rows) * (size_t)(number_of_columns);
    A->Row = NULL;
    A->Column = NULL;
    A->RowPtr = NULL;
    A->Value = NULL;
}


void Initialize_And_Set_Dense_Matrix(SPARSE_MATRIX *B, int number_of_rows, int number_of_columns, int option)
{

    Initialize_Dense_Matrix(B, number_of_rows, number_of_columns);
    B->Value = (float *)global_memory.small_malloc(B->number_of_nnzs * sizeof(float));


    //if (option == 1) { for(size_t q = 0; q < B->number_of_nnzs; q++) B->Value[q] = 1.00*((q % 17));} 
    if (option == 1) { for(size_t q = 0; q < B->number_of_nnzs; q++) B->Value[q] = 1.00*((1));} 
    else if (option == 0) { for(size_t q = 0; q < B->number_of_nnzs; q++) B->Value[q] = 0; }
    else ERROR_PRINT();
}





void Initialize_CX_Computation(void)
{
    //Each node parses its part of the file, and updates global_A accordingly...
    Parse_File_MTX(global_input_filename, &global_A);

#if 0
    {
        SPARSE_MATRIX *A = &global_A;
        PRINT_LIGHT_RED
        printf("rows = %d ::: columns = %d ::: nnzs = %lld\n", A->number_of_rows, A->number_of_columns, A->number_of_nnzs);
        printf("k_prime = %d\n", k_prime);
        printf("Mem Size of B = %.2lf MB\n", sizeof(float) * k_prime * A->number_of_columns/1000.0/1000.0);
        PRINT_BLACK
    }
#endif

    Compute_Analytical_Formula(k_prime/2);
    //B is  n * k_prime
    Initialize_And_Set_Dense_Matrix(&global_B,    global_A.number_of_columns, k_prime, 1);
    Initialize_And_Set_Dense_Matrix(&global_ATAB, global_A.number_of_columns, k_prime, 0);
    if (node_id == 0) Initialize_And_Set_Dense_Matrix(&global_ATAB_Reduced_Over_All_Nodes, global_A.number_of_columns, k_prime, 0);

    int nthreads = global_parallel.nthreads;

    global_ATAB_Per_Thread = (SPARSE_MATRIX **)global_memory.small_malloc(nthreads * sizeof(SPARSE_MATRIX *));

    for(int tid = 0; tid < nthreads; tid++)
    {
        global_ATAB_Per_Thread[tid] = (SPARSE_MATRIX *)global_memory.small_malloc(nthreads * sizeof(SPARSE_MATRIX));
        Initialize_And_Set_Dense_Matrix(global_ATAB_Per_Thread[tid], global_A.number_of_columns, k_prime, 0);
    }

    global_compute_atab_part1_per_thread_time = (unsigned long long int *)global_memory.small_malloc(nthreads * sizeof(unsigned long long int));
    global_compute_atab_part2_per_thread_time = (unsigned long long int *)global_memory.small_malloc(nthreads * sizeof(unsigned long long int));

    for(int tid = 0; tid < nthreads; tid++)
    {
        global_compute_atab_part1_per_thread_time[tid] = 0;
        global_compute_atab_part2_per_thread_time[tid] = 0;
    }
}



int global_argc;
char **global_argv;


void ParseArgs(int argc, char **argv)
{
  if (argc != 4)
  {
    printf("Usage ./a.out <nnodes> <filename.mtx> <nthreads>\n");
    exit(123);
  }

  if (sizeof(size_t) != 8) ERROR_PRINT();
  sscanf(argv[1], "%d", &nnodes);

  sprintf(global_input_filename, "%s", argv[2]);
  sscanf(argv[3], "%d", &nthreads);

  global_parallel.Set_nthreads(nthreads, &global_memory);
  sprintf(global_output_filename, "jjj123_%d_%d.txt", node_id, nthreads);

  global_argc = argc;
  global_argv = argv;

  PRINT_GREEN
  
  if (node_id == (nnodes -1))
  {
      printf("global_input_filename = %s\n", global_input_filename);
      printf("CORE_FREQUENCY = %.2lf GHz\n", CORE_FREQUENCY/1000.0/1000.0/1000.0);
      printf("nnodes = %d\n", nnodes);
  }
  //printf("global_number_of_records = %lld\n", global_number_of_records);
  //printf("global_filesize_bytes = %lld bytes (%.2lf GB)\n", global_filesize_bytes, global_filesize_bytes/1024.0/1024.0/1024.0);
  PRINT_BLACK
}


void Initialize_MPI_Stuff(int argc, char **argv)
{
#ifdef MULTIPLE_NODES
    int provided;

    MPI_Status stat;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided); /*START MPI */
    MPI_Comm_rank(MPI_COMM_WORLD, &node_id); /*DETERMINE RANK OF THIS PROCESSOR*/
    MPI_Comm_size(MPI_COMM_WORLD, &nnodes); /*DETERMINE TOTAL NUMBER OF PROCESSORS*/
    MPI_Errhandler_set(MPI_COMM_WORLD,MPI_ERRORS_RETURN);

    if (node_id == (nnodes-1)) { printf("Done Initialize_MPI()\n"); fflush(stdout);}
#endif
}

void Finalize_MPI_Stuff(void)
{
#ifdef MULTIPLE_NODES
    MPI_Finalize();

    if (node_id == (nnodes-1)) { printf("Done Finalize_MPI()\n"); fflush(stdout);}

#endif
}

  
void Spit_Files(void)
{
    char filename[128];

    SPARSE_MATRIX *A = &global_A;

    sprintf(filename, "%d.bin", node_id);

    FILE *fp = fopen(filename, "wb");

    if (node_id == 0)
    {
        fwrite(&(A->number_of_rows), sizeof(int), 1, fp);
        fwrite(&(A->number_of_columns), sizeof(int), 1, fp);
        fwrite(&(A->number_of_nnzs), sizeof(long long int), 1, fp);
    }

    for(long long int k = 0; k < A->mynode_number_of_nnzs; k++)
    {
        int x = A->Row[k] + 1;
        int y = A->Column[k] + 1;
        float z = A->Value[k];

        fwrite(&x, sizeof(int), 1, fp);
        fwrite(&y, sizeof(int), 1, fp);
        fwrite(&z, sizeof(float), 1, fp);
    }
    fclose(fp);
}
    
void Reduce_Matrix_For_Final_Answer(int threadid)
{
    int nthreads = global_parallel.nthreads;

    int rows_per_thread = (global_ATAB.number_of_rows % nthreads) ? (global_ATAB.number_of_rows / nthreads + 1) : global_ATAB.number_of_rows / nthreads;

    int starting_row_index = rows_per_thread * (threadid + 0);
    int   ending_row_index = rows_per_thread * (threadid + 1);

    if (starting_row_index > global_ATAB.number_of_rows) starting_row_index = global_ATAB.number_of_rows;
    if (  ending_row_index > global_ATAB.number_of_rows)   ending_row_index = global_ATAB.number_of_rows;

    if (global_ATAB.number_of_columns != k_prime) ERROR_PRINT();

    float *Temp1 = (float *)global_memory.small_malloc(k_prime * sizeof(float));

    for(int row_id = starting_row_index; row_id < ending_row_index; row_id++)
    {
        size_t offset = (size_t)(row_id) * (size_t)(k_prime);
        for(int q = 0; q < k_prime; q++) Temp1[q] = 0.0;
            
        for(int tid = 0; tid < nthreads; tid++)
        {
            float *Local_Address = global_ATAB_Per_Thread[tid]->Value + offset;
            for(int col_id = 0; col_id < k_prime; col_id++)
            {
                Temp1[col_id]  += Local_Address[col_id];
            }
        }

        for(int q = 0; q < k_prime; q++) global_ATAB.Value[offset + q] = Temp1[q];
    }
}


void Compute_ATAB(int threadid, SPARSE_MATRIX *A, SPARSE_MATRIX *B, SPARSE_MATRIX *Answer)
{
    if (A->is_sparse != 1) ERROR_PRINT();
    if (B->is_sparse != 0) ERROR_PRINT();

    //unsigned long long int stime = read_tsc();

    //B is n * k_prime 
#if 0
    int is_sparse;
    int number_of_rows, number_of_columns;
    long long int number_of_nnzs;
    int *Row;
    int *Column;
    float *Value;
    size_t *RowPtr;
#endif

    int kk_prime = B->number_of_columns;

    if (k_prime != kk_prime) ERROR_PRINT();

    float *Temp1 = (float *)global_memory.small_malloc(k_prime * sizeof(float));

    int starting_row_index = global_Starting_Row_Index[threadid];
    int   ending_row_index = global_Ending_Row_Index[threadid];

    for(int row_id = starting_row_index; row_id < ending_row_index; row_id++)
    {
        /////////////////////////////////////////////////////////////////////////////////////////////////////////////
        //Step 1: For each row Ai -- Compute Ai.B
        /////////////////////////////////////////////////////////////////////////////////////////////////////////////

        size_t starting_nnz_index = A->RowPtr[row_id + 0];
        size_t   ending_nnz_index = A->RowPtr[row_id + 1];

        int nnz = (int)(ending_nnz_index - starting_nnz_index);

        int *Row = A->Row    + starting_nnz_index;
        int *Col = A->Column + starting_nnz_index;
        float *A_Value = A->Value + starting_nnz_index;

        for(int k = 0; k < k_prime; k++) Temp1[k] = 0.0;
        //Working on row_id (th) row...

        //for(int j = 0; j < nnz; j++) if (Row[j] != row_id) ERROR_PRINT();

        unsigned long long int stime1 = read_tsc();

        for(int j = 0; j < nnz; j++)
        {
            int col_id = Col[j];

            float multiplier = A_Value[j];
            size_t offset = (size_t)(col_id) * (size_t)(k_prime);
            float *B_Value = B->Value + offset;

            for(int k = 0; k < k_prime; k++)
            {
                Temp1[k] += multiplier * B_Value[k];
            }
        }

        unsigned long long int etime1 = read_tsc();
        unsigned long long int ttime1 = etime1 - stime1;
        global_compute_atab_part1_per_thread_time[threadid] += ttime1;

        //Temp1 is [1 * k_prime] matrix


        /////////////////////////////////////////////////////////////////////////////////////////////////////////////
        //Step 2: For each row Ai -- Compute Ai.B
        /////////////////////////////////////////////////////////////////////////////////////////////////////////////

        unsigned long long int stime2 = read_tsc();
        for(int j = 0; j < nnz; j++)
        {
            int col_id = Col[j];
            float multiplier = A_Value[j];
            size_t offset = (size_t)(col_id) * (size_t)(k_prime);
            //float *C_Value = Answer->Value + 0;
            float *C_Value = Answer->Value + offset;

            for(int k = 0; k < k_prime; k++)
            {
                C_Value[k] += multiplier * Temp1[k];
            }
        }

        unsigned long long int etime2 = read_tsc();
        unsigned long long int ttime2 = etime2 - stime2;
        global_compute_atab_part2_per_thread_time[threadid] += ttime2;
    }

    //unsigned long long int etime = read_tsc();
    //unsigned long long int ttime = etime - stime;
    //global_compute_atab_time = ttime;
}


void Compute_Per_Thread_Starting_Ending_Index(int threadid)
{
    int nthreads = global_parallel.nthreads;

    if (threadid == 0)
    {
        global_Starting_Row_Index = (int *)global_memory.small_malloc(nthreads * sizeof(int));
        global_Ending_Row_Index =   (int *)global_memory.small_malloc(nthreads * sizeof(int));


        //HACK: For now, divide evenly...

        for(int tid = 0; tid < nthreads; tid++)
        {
            int rows_per_thread = (global_A.mynode_number_of_rows % nthreads) ? (global_A.mynode_number_of_rows / nthreads + 1) : global_A.mynode_number_of_rows / nthreads;

            int starting_row_index = rows_per_thread * (tid + 0);
            int   ending_row_index = rows_per_thread * (tid + 1);

            if (starting_row_index > global_A.mynode_number_of_rows) starting_row_index = global_A.mynode_number_of_rows;
            if (  ending_row_index > global_A.mynode_number_of_rows)   ending_row_index = global_A.mynode_number_of_rows;

            global_Starting_Row_Index[tid] = starting_row_index;
            global_Ending_Row_Index[tid]   = ending_row_index;
        }
    }
}

void *Perform_CX_Computation_Parallel(void *arg1)
{
    int threadid = (int)((size_t)(arg1));

    global_parallel.SetAffinity(threadid);

    global_parallel.barrier(threadid);

    //+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
    //Step 0: Compute the starting_row_index and ending_row_index for
    //each thread -- to perform ATAB
    //+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-

    Compute_Per_Thread_Starting_Ending_Index(threadid);
    global_parallel.barrier(threadid);

    //+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
    //Step 1: Compute the local ATAB
    //+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
    Compute_ATAB(threadid, &global_A, &global_B, global_ATAB_Per_Thread[threadid]);
    global_parallel.barrier(threadid);

    //+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
    //Step 2:  Reduce the individual matrices to the final matrix...
    //+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-
    Reduce_Matrix_For_Final_Answer(threadid);
    global_parallel.barrier(threadid);


    return arg1;

}

void Perform_CX_Computation(void)
{
    int nthreads = global_parallel.nthreads;
    pthread_t *threads = global_parallel.threads;

    unsigned long long int stime = read_tsc();

    for(long long int k=1; k<nthreads; k++) pthread_create(&threads[k], NULL, Perform_CX_Computation_Parallel, (void *)(k));
    Perform_CX_Computation_Parallel(0);
    for(long long int k=1; k<nthreads; k++) pthread_join(threads[k], NULL);

    unsigned long long int etime = read_tsc();
    unsigned long long int ttime = etime - stime;
    global_compute_atab_time = ttime;
}

#define FORMAT_PRINT(X) X, X/CORE_FREQUENCY, (X*100.0)/global_compute_atab_time

void Print_Memory_Stats(void)
{
    int nthreads = global_parallel.nthreads;

    global_compute_atab_part1_time = 0;
    for(int tid = 0; tid < nthreads; tid++) global_compute_atab_part1_time = TBD_MAX(global_compute_atab_part1_time, global_compute_atab_part1_per_thread_time[tid]);
    global_compute_atab_part2_time = 0;
    for(int tid = 0; tid < nthreads; tid++) global_compute_atab_part2_time = TBD_MAX(global_compute_atab_part2_time, global_compute_atab_part2_per_thread_time[tid]);

    unsigned long long int compute_atab_part1_avg_time = 0;
    unsigned long long int compute_atab_part2_avg_time = 0;
    for(int tid = 0; tid < nthreads; tid++) compute_atab_part1_avg_time += global_compute_atab_part1_per_thread_time[tid]; compute_atab_part1_avg_time/= nthreads;
    for(int tid = 0; tid < nthreads; tid++) compute_atab_part2_avg_time += global_compute_atab_part2_per_thread_time[tid]; compute_atab_part2_avg_time/= nthreads;

    for(int tid = 0; tid < nthreads; tid++) printf("%.2lf ", global_compute_atab_part1_per_thread_time[tid]/CORE_FREQUENCY); printf("\n");
    for(int tid = 0; tid < nthreads; tid++) printf("%.2lf ", global_compute_atab_part2_per_thread_time[tid]/CORE_FREQUENCY); printf("\n");

    size_t malloced_memory_size = global_memory.small_malloced_memory + global_memory.large_malloced_memory;

    float bytes_per_element = (malloced_memory_size * 1.0)/global_A.number_of_nnzs;
    printf("Malloced Memory = %.2lu Bytes (%.2lf GB) -- (%.2lf bytes per nz)\n", malloced_memory_size, malloced_memory_size/1000.0/1000.0/1000.0, bytes_per_element);
PRINT_BLUE
    //printf("MAX_ITERATIONS = %d\n", MAX_ITERATIONS);
    //printf("Time Taken For M0*M1'      = %lld cycles (%.2lf seconds) [%.2lf%%]\n", FORMAT_PRINT(global_compute_product_matrix_matrix_transpose_time));
    //printf("Time Taken For M'*M        = %lld cycles (%.2lf seconds) [%.2lf%%]\n", FORMAT_PRINT(global_compute_product_matrix_selfmatrix_transpose_time));
    //printf("Time Taken For M0*SM1      = %lld cycles (%.2lf seconds) [%.2lf%%]\n", FORMAT_PRINT(global_compute_product_matrix_sparse_matrix_time));
    printf("CORE_FREQUENCY = %.2lf GHz \n", CORE_FREQUENCY/1000.0/1000.0/1000.0);
    printf("Time Taken For ATAB_Part1  =  %lld cycles ( %.2lf seconds) [ %.2lf%%]\n", FORMAT_PRINT(global_compute_atab_part1_time));
    printf("Time Taken For ATAB_Part2  =  %lld cycles ( %.2lf seconds) [ %.2lf%%]\n", FORMAT_PRINT(global_compute_atab_part2_time));
    printf("(Avg.) Time Taken For ATAB_Part1  =  %lld cycles ( %.2lf seconds) [ %.2lf%%]\n", FORMAT_PRINT(compute_atab_part1_avg_time));
    printf("(Avg.) Time Taken For ATAB_Part2  =  %lld cycles ( %.2lf seconds) [ %.2lf%%]\n", FORMAT_PRINT(compute_atab_part2_avg_time));
//
PRINT_BROWN
    //printf("Total Time Taken For Matrix Factorization  = %lld cycles (%.2lf seconds)\n", global_matrix_factorization_time, global_matrix_factorization_time/CORE_FREQUENCY);
    printf("Timeee Taken For ATAB (%s)  =  %lld cycles ( %.2lf seconds) [ %.2lf%%]\n", global_argv[1], FORMAT_PRINT(global_compute_atab_time));
PRINT_BLACK

    fflush(stdout);
    fflush(stdout);
    fflush(stdout);
}

void Spit_File_MTX(char *filename, SPARSE_MATRIX *A)
{

    FILE *fp = fopen(filename, "w");

    fprintf(fp, "%%%%MatrixMarket matrix coordinate real general\n");

    fprintf(fp, "%d %d %lld\n", A->number_of_rows, A->number_of_columns, A->number_of_nnzs);


    if (A->is_sparse == 1)
    {
        for(long long int k = 0; k < A->number_of_nnzs; k++)
        {
            fprintf(fp, "%d %d %f\n", A->Row[k] + 1, A->Column[k] + 1, A->Value[k]);
        }
    }
    else
    {
        long long int k = 0;
        for(int row_id = 0; row_id < A->number_of_rows; row_id++)
        {
            for(int col_id = 0; col_id < A->number_of_columns; col_id++)
            {
                fprintf(fp, "%d %d %f\n", row_id + 1, col_id + 1, A->Value[k]);
                k++;
            }
        }


    }

    fclose(fp);
}



void Spit_Final_Output(void)
{
    Print_Memory_Stats();

    printf("Spitting out %s...\n", global_output_filename);
    Spit_File_MTX(global_output_filename, &global_ATAB);


    if (node_id == 0) Spit_File_MTX("ttt.txt", &global_ATAB_Reduced_Over_All_Nodes);
    //Spit_File_MTX(global_output_filename, &global_B);
}

  
void Reduce_Output_Over_All_Nodes(void)
{
    MPI_BARRIER(node_id);

    int number_of_nnzs = (int)(global_ATAB.number_of_nnzs);

    MPI_Reduce(global_ATAB.Value,  global_ATAB_Reduced_Over_All_Nodes.Value, number_of_nnzs, MPI_FLOAT, MPI_SUM, 0, MPI_COMM_WORLD);



}


int main(int argc, char **argv)
{

  //+++ MPI init +++++++++++++++
  Initialize_MPI_Stuff(argc, argv);

  ParseArgs(argc, argv);

  Initialize_CX_Computation();

  //Spit_Files();

  Perform_CX_Computation();

  Reduce_Output_Over_All_Nodes();

  Spit_Final_Output();

#if 0
  unsigned long long int start_time, end_time, total_time;
  double seconds;
#endif


  //+++ MPI finalize +++++++++++++++
  Finalize_MPI_Stuff();


}
