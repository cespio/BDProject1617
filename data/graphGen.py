#Alessandro Rizzuto - Francesco Contaldo 2017 Data Mining project
#graphGenerator python
#The parameter e represents the max number of possible repeated edges

import numpy
from random import uniform
import sys

#no labels foreign and outside no sense
labels={
    "1": "Urban",
    "2": "Movement",
    "3": "Nature",
    "4": "Social",
    "5": "Commerical",
    "6": "Road",
    "7": "Sport",
    "8": "Young",
    "9": "Elderly",
    "10": "Historical",
    "11": "Student",
    "12": "Mountain",
    "13": "Sea"
}


def writeLables(lab,file_out,l):
    for x in lab:
        n=uniform(1,l)
        lab[x]=labels[str(int(n))]
        file_out.write(str(x) + " [ label = \" "+lab[x]+" \" ] \n")


def graphGenerator(n,h,l):
    file_out=open("graphGenOut.dot","w")
    file_out.write("digraph prova{\n")
    flag=0
    labels_node={}
    matrix=numpy.zeros((n,n))
    for y in range(n):
        for z in range(n):
            n1=uniform(1,100)
            n2=uniform(1,100)
            if(n1>n2 and matrix[y][z]==0 and y!=z):
                time=str(int(uniform(0,h)))
                matrix[y][z]=1
                labels_node[y]=None
                labels_node[z]=None
                file_out.write(str(y) + " -> " + str(z) + "  [label=\"" + time + "\"];\n")
                flag=1
    writeLables(labels_node,file_out,l)
    file_out.write("}")
    file_out.close()

def main():
    n=int(sys.argv[1])
    h=int(sys.argv[2])
    l=int(sys.argv[3])
    graphGenerator(n,h,l)
    print("Graph Generated")

if __name__ == "__main__":
    main()
