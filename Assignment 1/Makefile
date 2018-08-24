all: normal opt

normal: normal.o 
	g++ --std=c++14 normal.o -o normal.out -lpthread

opt: optimization.o
	g++ --std=c++14 optimization.o -o optimization.out -lpthread

normal.o:
	g++ --std=c++14 -c normal.cpp -o normal.o -lpthread

optimization.o:
	g++ --std=c++14 -c optimization.cpp -o optimization.o -lpthread


test:
	g++ --std=c++14 test.cpp -o test.out -lpthread -g