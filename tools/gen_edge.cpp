#include<bits/stdc++.h>
using namespace std;

const int MAXR=1e5;

int ran(int a,int b) {
	uniform_int_distribution<int> u(a,b);
	static default_random_engine e(time(0));
	return u(e);
}

int main(int argc,char *argv[]) {
	int n=atoi(argv[1]);
	double p=atof(argv[2]);
	
	// initialize the random generator
	ran(1,100);
	ran(1,100);
	ran(1,100);
	
	// generate the edge list
	int degree_of_2=0;
	FILE *f=fopen("edge_list.csv","w");
	fprintf(f,"u,v\n");
	for(int i=1; i<=n; i++)
		for(int j=i+1; j<=n; j++) {
			double x=ran(0,MAXR)*1.0/MAXR;
			if (x<=p) {
				fprintf(f,"%d,%d\n",i,j);
				degree_of_2+=(i==2 || j==2);
			}
		}
	fclose(f);
	
	printf("For you to check: the degree of vertex 2 is %d\n",degree_of_2);
}