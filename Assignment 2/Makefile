CXX=mpic++
RUN=mpirun
N=10

all: CL LY

CL:
	$(CXX) CL.cpp -o CL.out -lpthread
LY:
	$(CXX) LY.cpp -o LY.out -lpthread

run_CL:
	mpirun -n $(N) CL.out > CL_logs.txt

run_LY:
	mpirun -n $(N) LY.out > LY_logs.txt

gen_input:
	python gen.py $(N) > inp-params.txt

clean:
	rm -f *.out *logs.txt snapshot_dump*