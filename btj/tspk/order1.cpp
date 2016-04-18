// Program for reordering rows of matrix, using the TSP solution.
// This program creates a matrix of only the data values.  For a
// matrix that has the cluster borders, use order2.  For a rearrangement
// of gene names, use orderNames.
//
// Sharlee Climer, 2004
//
#include "conv.h"
using namespace std;
int main(int argc, char * argv[])
{
  FILE *tourFile;
  FILE *input;
  FILE *output;

  if (argc != 7) {
    cerr << "Usage: order1 tour.sol data.txt outputFile.txt numClusters numObjects numFeatures" << endl;
    fatal("Check command line arguments");
  }

  if ((tourFile = fopen(argv[1], "r")) == NULL)
    fatal("File could not be opened.\n");

  if (((input = fopen(argv[2], "r")) == NULL) || 
      ((output = fopen(argv[3], "w")) == NULL))
    fatal("File could not be opened.\n");

  int K = atoi(argv[4]);
  int n = atoi(argv[5]);     // number of items
  int numConditions = atoi(argv[6]);  // number of conditions
  cout << "n = " << n << ", numConditions = " << numConditions << ", startCity = " << startCity << endl;

  char string[50];  
  int num;
  int * tour;
  float ** data;
  float ** newData;
  int dummy = -1;
  int *partition;
  float *gVals;
  int dummyPtr = 0;
  int rowPtr = 0;
  int colPtr = 0;

  if (((partition = new int[K-1]) == NULL) || 
      ((gVals = new float[K-1]) == NULL))
    fatal("Memory not allocated");

  if (((tour = new int[n+K]) == NULL) ||
      ((data = new float* [n]) == NULL))
    fatal("Memory not allocated");

  for (int i = 0; i < n; i++)
    if ((data[i] = new float[numConditions]) == NULL)
      fatal("Memory not allocated");

  if ((newData = new float* [n]) == NULL)
    fatal("Memory not allocated");

  for (int i = 0; i < n; i++)
    if ((newData[i] = new float[numConditions]) == NULL)
      fatal("Memory not allocated");

  fscanf(tourFile, "%s", string);   // throw away number of cities
  num = atoi(string);
  if (num != n+K)
    fatal("Number of cities does not match");
   
  for (int i = 0; i < n+K; i++) {
    fscanf(tourFile, "%s", string);
    tour[i] = atoi(string);
    if (startCity)
      tour[i]--;     // subtract if city numbers 1 to n
    if ((tour[i] < 0) || (tour[i] >= n+K))
      fatal("Check startCity in convert.h");
    if ((tour[i] >= n) && (dummy == -1))   // find first dummy city
      dummy = i;
  }
  fclose(tourFile);
  /* 
  for (int i = 0; i < n; i++) 
    for (int j = 0; j < numConditions; j++) {      // read in gene data
      fscanf(input, "%s", string);
      data[i][j] = atof(string);
    }
  */
  
   ifstream infile;
 infile.open (argv[2]);
std::string tmp;
int x = 0;
while(!infile.eof()) // To get you all the lines.
{
  getline(infile,tmp); // Saves the line in STRING.
  istringstream ss(tmp);

  std::string token;
  int y = 0;
  while(getline(ss, token, ','))
  {
    data[x][y] = atof(token.c_str());
    y++;
  }
  x++;
}
infile.close();
  
  
  
  for (int i = dummy+1; i < n+K; i++) {
    if (tour[i] >= n) {          // mark other dummy nodes
      partition[dummyPtr++] = rowPtr - 1;
      continue;                  // don't include dummy nodes in arrangement
    }

    for (int j = 0; j < numConditions; j++) {
      newData[rowPtr][colPtr++] = data[tour[i]][j];
      if (data[tour[i]][j] > 999)
	fprintf(output, "0 ");
      else
	fprintf(output, "%.0f,", data[tour[i]][j]);
    }
    colPtr = 0;
    rowPtr++;
    fprintf(output, "\n");
  }

  for (int i = 0; i < dummy; i++) {
    for (int j = 0; j < numConditions; j++) {
      newData[rowPtr][colPtr++] = data[tour[i]][j]; 
      if (data[tour[i]][j] > 999)
	fprintf(output, "0 ");
      else  
	fprintf(output, "%.0f,", data[tour[i]][j]);
    }
    colPtr = 0;
    rowPtr++;   
    fprintf(output, "\n");
  }

  fclose(output);
  fclose(input);

  delete [] tour;
  for (int i = 0; i < n; i++) {
    delete [] data[i];
    delete [] newData[i];
  }
  delete [] data;
  delete [] newData;

  return 1;
}
