CXX=mpic++
RUN=mpirun
N=10

# Build
all: build_Raymond build_SuzukiKasami

build_Raymond:
	$(CXX) ProgAssn3-CS15BTECH11018-Raymond.cpp -o Raymond.out -lpthread

build_SuzukiKasami:
	$(CXX) ProgAssn3-CS15BTECH11018-SuzukiKasami.cpp -o SuzukiKasami.out -lpthread

# Run
run_Raymond:
	$(RUN) -n $(N) Raymond.out > Raymond_logs.txt

run_SuzukiKasami:
	$(RUN) -n $(N) SuzukiKasami.out > SuzukiKasami_logs.txt

# Sort Logs
sort_logs: sort_raymond_logs sort_suzuki_kasami_logs

sort_raymond_logs:
	python sort_logs.py Raymond_logs.txt > Sorted_Raymond_logs.txt 	
	rm Raymond_logs.txt
	mv Sorted_Raymond_logs.txt Raymond_logs.txt

sort_suzuki_kasami_logs:
	python sort_logs.py SuzukiKasami_logs.txt > Sorted_SuzukiKasami_logs.txt 	
	rm SuzukiKasami_logs.txt
	mv Sorted_SuzukiKasami_logs.txt SuzukiKasami_logs.txt

# Generate inputs
gen_input:
	python gen.py $(N) > inp-params.txt

# Clean logs and executables
clean:
	rm -f *.out *logs.txt
