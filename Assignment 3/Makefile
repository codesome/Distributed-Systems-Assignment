CXX=mpic++
RUN=mpirun
N=10

all: build_Raymond build_SusukiKasami

build_Raymond:
	$(CXX) Raymond.cpp -o Raymond.out -lpthread

build_SusukiKasami:
	$(CXX) SusukiKasami.cpp -o SusukiKasami.out -lpthread

Raymond:
	mpirun -n $(N) Raymond.out > Raymond_logs.txt

SusukiKasami:
	mpirun -n $(N) SusukiKasami.out > SusukiKasami_logs.txt

sort_logs: sort_raymond_logs sort_susuki_kasami_logs

sort_raymond_logs:
	python sort_logs.py Raymond_logs.txt > Sorted_Raymond_logs.txt 	
	rm Raymond_logs.txt
	mv Sorted_Raymond_logs.txt Raymond_logs.txt

sort_susuki_kasami_logs:
	python sort_logs.py SusukiKasami_logs.txt > Sorted_SusukiKasami_logs.txt 	
	rm SusukiKasami_logs.txt
	mv Sorted_SusukiKasami_logs.txt SusukiKasami_logs.txt

gen_input:
	python gen.py $(N) > inp-params.txt

clean:
	rm -f *.out *logs.txt
