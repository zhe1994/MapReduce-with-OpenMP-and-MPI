/*
	*	ECE563 Large Project ver 1.0
	*	Zhe Zeng
	*	03-31-2017
	*	There is no condition statement for different nodes/threads
	*	i.e. you need to change some value or comment some code blocks manually
*/

#include <iostream>
#include <unordered_map>
#include <omp.h>
#include <fstream>
#include <string>
#include <vector>
#include <queue>
#include <sstream>
#include <utility>
using namespace std;
#include <mpi.h>

#define NUMOFTHREADS 4
#define MIN_READER 0
#define MAX_READER 1
#define MIN_MAPPER 2
#define MAX_MAPPER 3
#define WORKSIZE 80

typedef unordered_map<string, int> MR_Mapper;
typedef MR_Mapper::iterator MR_Mapper_It;
typedef pair<string, int> MR_Mapper_Member;
typedef queue<string> MR_Database;
typedef queue<MR_Mapper_Member> MR_Reducer;
typedef pair<vector<string>, vector<int>> MR_Buffer;

void ReduceIt(MR_Mapper &res, MR_Mapper_Member toReduce){
	MR_Mapper_It it = res.find(toReduce.first);
	if(it != res.end()){
		(it->second) += (toReduce.second);
	}else{
		res.insert(MR_Mapper_Member(toReduce.first, toReduce.second));
	}
}

void MapIt(MR_Mapper &mapper, string toMap){
	MR_Mapper_It it = mapper.find(toMap);
	if(it != mapper.end()){
		++(it->second);
	}else{
		mapper.insert(MR_Mapper_Member(toMap, 1));
	}
}

bool CheckWorkStatus(vector<bool> status){
	bool res = true;
	for(int i = 0; i < status.size(); i++){
		if(!status[i]) return false;
	}
	return res;
}

void SendrcvVecString(int count, int dest, int source, vector<string> vec_toSend, vector<string> &vec_toRcv){
	for(int i = 0; i < vec_toSend.size(); ++i){
		MPI_Send(vec_toSend[i].c_str(), vec_toSend[i].length(), MPI_CHAR, dest, 0, MPI_COMM_WORLD);
	}
	for(int i = 0; i < count; ++i){
		MPI_Status status;
		MPI_Probe(source, 0, MPI_COMM_WORLD, &status);
		int len;
		MPI_Get_count(&status, MPI_CHAR, &len);	
		vec_toRcv[i].resize(len);
		MPI_Recv(&vec_toRcv[i][0], len, MPI_CHAR, source, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	}
}

int main(int argc, char* argv[]){
	int rank, size;
	
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	
	MR_Database db_words;
	MR_Mapper res, mpi_res;
	MR_Reducer localBuffer;
	vector<MR_Buffer>	rank_sdbuffer, rank_rcvbuffer;
	vector<MR_Reducer>	tid_reducer;
	vector<bool> isWorkDone;
	int nreducers = MAX_READER - MIN_READER + 1;
	int nmappers = MAX_MAPPER - MIN_MAPPER + 1;
	
	tid_reducer.resize(nreducers);
	for(int i = 0; i < nmappers; i++){
		isWorkDone.push_back(false);
	}
	
	rank_sdbuffer.resize(size);
	rank_rcvbuffer.resize(size);
	
	bool isReaderWorking = true;
	int nextWork = -1;
	int offset = 0;
	int workPerRank = WORKSIZE / size;
	int workToRead = workPerRank;
	
	/*Reader&Mapper Lock*/
	omp_lock_t db_wordsLock_rm;
	omp_init_lock(&db_wordsLock_rm);
	/*Reducer Lock*/
	omp_lock_t db_reducerLock_re[MAX_READER - MIN_READER + 1];
	for(int i = 0; i < nreducers; i++)
		omp_init_lock(&db_reducerLock_re[i]);
	/*Writter Lock*/
	omp_lock_t db_writerLock_w;
	omp_init_lock(&db_writerLock_w);
	omp_set_num_threads(NUMOFTHREADS);
	
	if(NUMOFTHREADS!=1){
		#pragma omp parallel shared(db_words)
		{
			MR_Mapper mapper;
			int tid = omp_get_thread_num();

			/*	Reader	*/
			
			if(tid < MIN_MAPPER){
				int _currentWork;
				int _maxWork = workPerRank;
				while(1){
					if(!workToRead){
						#pragma omp critical
						{
							isReaderWorking = false;	
						}
						break;
					}else{
						#pragma omp critical
						{
							++nextWork;
							--workToRead;
						}
						_currentWork = nextWork;
					}
					
					stringstream ss;
					ss<<"./"<<rank<<"/"<<"0"<<_currentWork << ".txt";
					string filename;
					ss>> filename;
					
					string word;
					ifstream ifs;
					ifs.open(filename);
					fflush(stdout);
					if(ifs.is_open()){
						while(ifs>>word){
							omp_set_lock(&db_wordsLock_rm);
							db_words.push(word);
							omp_unset_lock(&db_wordsLock_rm);
						}
					}else{
						cout<<" failed to open"<<endl;
						fflush(stdout);
					}
					ifs.close();												
					
				}
			}
			
			/*	Mapper	Parallelly*/
			if(tid >= MIN_MAPPER && tid <= MAX_MAPPER){			
				while(!db_words.empty()||isReaderWorking){
					string toMap = "";
					omp_set_lock(&db_wordsLock_rm);
					if(!db_words.empty()){
						toMap = db_words.front();
						db_words.pop();					
						omp_unset_lock(&db_wordsLock_rm);
						MapIt(mapper, toMap);
					}else{
						omp_unset_lock(&db_wordsLock_rm);
					}
					
				}			
			}
				
			#pragma omp barrier
			omp_destroy_lock(&db_wordsLock_rm);

			/*Mapper to Reducer*/
			if(tid >= MIN_MAPPER && tid <= MAX_MAPPER){
				for(MR_Mapper_It x = mapper.begin(); x!=mapper.end(); x++){
					MR_Mapper::hasher fn;
					int index = fn(x->first)%nreducers;
					omp_set_lock(&db_reducerLock_re[index]);				
					tid_reducer[index].push(MR_Mapper_Member(x->first, x->second));
					omp_unset_lock(&db_reducerLock_re[index]);
				}
				isWorkDone[tid%nmappers] = true;
			}
			/*Reduction and Writter*/
			
			if(tid<MIN_MAPPER){
				int index = tid%nreducers;
				while(!tid_reducer[index].empty() || !CheckWorkStatus(isWorkDone)){
					omp_set_lock(&db_reducerLock_re[index]);
					if(!tid_reducer[index].empty()){
						MR_Mapper_Member toReduce(tid_reducer[index].front());
						tid_reducer[index].pop();
						omp_unset_lock(&db_reducerLock_re[index]);
						omp_set_lock(&db_writerLock_w);
						ReduceIt(res, toReduce);
						omp_unset_lock(&db_writerLock_w);
					}else{
						omp_unset_lock(&db_reducerLock_re[index]);
					}			
				}		
			}
			#pragma omp barrier
		}
	}else{
		#pragma omp parallel shared(db_words) num_threads(1)
		{
			MR_Mapper mapper;
			int tid = 0;
			int _currentWork;
			int _maxWork = workPerRank;
			while(1){
				if(!workToRead){
					#pragma omp critical
					{
						isReaderWorking = false;	
					}
					break;
				}else{
					#pragma omp critical
					{
						++nextWork;
						--workToRead;
					}
					_currentWork = nextWork;
				}
				
				stringstream ss;
				ss<< rank << _currentWork << ".txt";
				string filename;
				ss>> filename;
				
				string word;
				ifstream ifs;
				ifs.open(filename);
				fflush(stdout);
				if(ifs.is_open()){
					while(ifs>>word){
						db_words.push(word);
					}
				}else{
					cout<<" failed to open"<<endl;
					fflush(stdout);
				}
				ifs.close();															
			}		
			
			while(!db_words.empty()||isReaderWorking){
				string toMap = "";
				if(!db_words.empty()){
					toMap = db_words.front();
					db_words.pop();					
					MapIt(mapper, toMap);
				}
				
			}
			
			omp_destroy_lock(&db_wordsLock_rm);
			/*Mapper to Reducer*/
			for(MR_Mapper_It x = mapper.begin(); x!=mapper.end(); x++){				
				tid_reducer[0].push(MR_Mapper_Member(x->first, x->second));
			}
			isWorkDone[0] = true;
			
			/*Reduction and Writter*/
			while(!tid_reducer[0].empty()||!isWorkDone[0]){
				if(!tid_reducer[0].empty()){
					MR_Mapper_Member toReduce(tid_reducer[0].front());
					tid_reducer[0].pop();
					ReduceIt(res, toReduce);
				}
			}
			#pragma omp barrier
		}		
	}
	
	omp_destroy_lock(&db_writerLock_w);
	for(int i = 0; i < nreducers; i++)
		omp_destroy_lock(&db_reducerLock_re[i]);
	
	for(MR_Mapper_It it = res.begin(); it != res.end(); it++){
		MR_Mapper::hasher fn;
		int index = fn(it->first)%size;
		rank_sdbuffer[index].first.push_back(it->first);
		rank_sdbuffer[index].second.push_back(it->second);
	}
	
	vector<int> rcvCount;
	rcvCount.resize(size);
	
	MPI_Barrier(MPI_COMM_WORLD);
	
	for(int i = 0; i < size; i++){
		if(i == rank){
			rcvCount[rank] = rank_sdbuffer[rank].first.size();
		}else{
			int buffer_size = rank_sdbuffer[i].first.size();
			MPI_Sendrecv(&buffer_size, 1, MPI_INT, i, 0, 
			&rcvCount[i], 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		}
	}
	MPI_Barrier(MPI_COMM_WORLD);
	
	for(int i = 0; i < size; i++){
		rank_rcvbuffer[i].first.resize(rcvCount[i]);
		rank_rcvbuffer[i].second.resize(rcvCount[i]);
	}	
	
	MPI_Barrier(MPI_COMM_WORLD);
	
	for(int i = 0; i < size; i++){
		if(i == rank){
			rank_rcvbuffer[rank].first = rank_sdbuffer[rank].first;
			rank_rcvbuffer[rank].second = rank_sdbuffer[rank].second;
		}else{
			int size = rank_sdbuffer[i].second.size();
			MPI_Sendrecv(&rank_sdbuffer[i].second[0], rank_sdbuffer[i].second.size(), MPI_INT, i, 0, 
			&rank_rcvbuffer[i].second[0], rcvCount[i], MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			SendrcvVecString(rcvCount[i], i, i, rank_sdbuffer[i].first, rank_rcvbuffer[i].first);						
		}
	}
	
	MPI_Barrier(MPI_COMM_WORLD);
	
	omp_lock_t mpi_res_w;
	omp_init_lock(&mpi_res_w);
	int o_index;
	#pragma omp parallel for num_threads(NUMOFTHREADS) shared(o_index)
	for(o_index = 0; o_index < size; o_index++){
		for(int j = 0; j < rank_rcvbuffer[o_index].first.size(); ++j){
			string t_word = rank_rcvbuffer[o_index].first[j];
			int t_count = rank_rcvbuffer[o_index].second[j];
			omp_set_lock(&mpi_res_w);
			MR_Mapper_It it = mpi_res.find(t_word);
			if(it != mpi_res.end())	it->second += t_count;
			else	mpi_res.insert(MR_Mapper_Member(t_word, t_count));
			omp_unset_lock(&mpi_res_w);			
		}
	}
	#pragma omp barrier
	omp_destroy_lock(&mpi_res_w);
	
	stringstream res_ss;
	res_ss<< "FinalResult_" << rank << ".txt";
	string res_filename;			
	res_ss>>res_filename;
	ofstream res_ofs(res_filename);
	if(res_ofs.is_open()){
		for(MR_Mapper_It it = mpi_res.begin(); it != mpi_res.end(); it++){
			string w_word = it->first;
			int w_count = it->second;
			res_ofs<< "<"<<w_word<<", "<<w_count<<">"<<endl;
		}
	}
	res_ofs.close();

	MPI_Barrier(MPI_COMM_WORLD);	
	MPI_Finalize();
	
	return 0;
}