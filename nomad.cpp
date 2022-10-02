#include <iostream>
#include <random>
#include <upcxx/upcxx.hpp>
#include <math.h>
#include <vector> 
#include <map>
#include <string>
#include <fstream>
#include <numeric>
#include <algorithm>
#include <functional>
#include <unistd.h>
#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_priority_queue.h>
#include <thread>
#include <chrono>
#include <ctime>
using namespace std;

template <typename T> std::vector<T> operator+(const std::vector<T>& a, const std::vector<T>& b){
    UPCXX_ASSERT(a.size() == b.size());

    std::vector<T> result;
    result.reserve(a.size());

    std::transform(a.begin(), a.end(), b.begin(), std::back_inserter(result), std::plus<T>());
    return result;
}
template <typename T> std::vector<T> operator-(const std::vector<T>& a, const std::vector<T>& b){
    UPCXX_ASSERT(a.size() == b.size());

    std::vector<T> result;
    result.reserve(a.size());

    std::transform(a.begin(), a.end(), b.begin(), std::back_inserter(result), std::minus<T>());
    return result;
}
template <typename T> T operator*(const std::vector<T>& a, const std::vector<T>& b){
    return std::inner_product(a.begin(), a.end(), b.begin(), 0);
}
template <typename T> std::vector<T> operator*(const T scalar, const std::vector<T>& a){
    std::vector<T> result(a.size());
    for(int idx=0; idx<a.size(); idx++){
        result[idx] = scalar * a[idx];
    }
    return result;
}

class ColumnData {
public:
    int item_index;
    std::vector<double> values;
    int perm_index;
    ColumnData(){
        item_index = 0;
        perm_index = 0;
    }
    UPCXX_SERIALIZED_FIELDS(item_index, values, perm_index)
};
class RatingHashMap {       
private:
    using rating_map = std::unordered_map<int, std::unordered_map<int, std::pair<double, int>>>; // store <item_id, <user_id, <rating, t>>>>
    rating_map local_map;

public:    
RatingHashMap () : local_map({}){};
    void insert(const int &user_index, const int &item_index, const double &rating){ // 
        auto it = local_map.find(item_index);
        if (it == local_map.end()){	
            std::unordered_map<int, std::pair<double, int>> new_entry{};
            std::pair<double, int> rating_and_update;
            rating_and_update.first = rating;
            rating_and_update.second = 0;
            new_entry.insert({user_index, rating_and_update});
            local_map.insert({item_index, new_entry});
        } else{
            std::unordered_map<int, std::pair<double, int>> old_entry = it->second;
            std::pair<double, int> rating_and_update;
            rating_and_update.first = rating;
            rating_and_update.second = 0;
            it->second.insert({user_index, rating_and_update});
        }                  
    }
    void print_map(){
        int count = 0;
        for(auto it:local_map){
            for(auto it1:it.second){
                count++;
            }
        }
        cout << count << "\n";
    }

    std::unordered_map<int, std::pair<double, int>> get_by_item(const int &item_index){
        auto it = local_map.find(item_index);
        if (it == local_map.end()){
            std::unordered_map<int, std::pair<double, int>> empty_entry{};
            return empty_entry;
        }
        return it->second;
    }
    void increase_num_updates(const int &user_idx, const int &item_idx){
        auto it = local_map.find(item_idx);
        if (it == local_map.end()){    
            cout << "[ERROR] DONE HAVE (" << user_idx << "," << item_idx << ")\n";
        } else{
            std::unordered_map<int, std::pair<double, int>> old_entry = it->second;
            auto it2 = it->second.find(user_idx);
            if (it2 == it->second.end()){
                cout << "[ERROR] DONE HAVE-- (" << user_idx << "\n";
            }else{
                it2->second.second += 1; 
            }
        }
    }
};

class JobQueueList {       
private:
    using d_queues_list = upcxx::dist_object<std::vector<tbb::concurrent_queue<ColumnData>>>; // store <item_id, <user_id, <rating, t>>>>
    d_queues_list local_job_queues_list;
public:    
    JobQueueList (const int &n_threads) : local_job_queues_list({}){
        (*local_job_queues_list).resize(n_threads);
    };
    upcxx::future<>  push_remote(const int &next_rank, const int &thread_id, const ColumnData & column_data ){ // 
        return upcxx::rpc(next_rank, [](d_queues_list &l_job_queues_list, const int &t_id, const ColumnData & column_data) // 
            {   
                (*l_job_queues_list)[t_id].push(column_data);
            }, local_job_queues_list, thread_id, column_data);
    }
    void push_local(const int &thread_id,const ColumnData & column_data){ // 
        (*local_job_queues_list)[thread_id].push(column_data);
    }
    bool try_pop(const int &thread_id, ColumnData & column_data){ // 
        return (*local_job_queues_list)[thread_id].try_pop(column_data);
    }
};

int main(int argc, char **argv) {
    upcxx::init();
    const double lambda = 0.05; // regularization
    const double decay_rate = 0.012;
    const double learning_rate = 0.0001;
    const int n_retries = 1; // number of circulating a (i, j) in a machine
    const double epsilon = 0.0000001; // stop threshold
    const int UNITS_PER_MSG = 100;
    const int n_threads_per_machines = 3;
    const int MAX_UPDATES = 100000; // Assume that this division is even

    std::thread *computing_threads[n_threads_per_machines];
    std::atomic<int> buffer_count(1);
    std::atomic<bool> sending_signal(false);
    upcxx::dist_object<std::vector<std::vector<std::tuple<double, long, long>>>> distributed_losses({}); /// save <total loss and loss count>
    (*distributed_losses).resize(n_threads_per_machines);
    upcxx::dist_object<std::vector<int>> training_steps({});
    (*training_steps).resize(n_threads_per_machines);
    upcxx::dist_object<bool> stop_signal = false;
    // for netflix
    // int m = 2649429;
    // int n = 17770;
    // int k = 100;
    // char delimiter = '\t'; 

    // for ml-20m
    // int m = 138493;
    // int n = 27278;
    // int k = 100;
    // char delimiter = ','; 
   
    //for ml-100k
    // int m = 943;
    // int n = 1682;
    // int k = 20;
    // char delimiter = '\t';

    // for ml-10m
    int m = 71567;
    int n = 10681;
    int k = 100;
    char delimiter = ','; 

    int block_size = m/(upcxx::world().rank_n() * n_threads_per_machines);
    default_random_engine generator;
    uniform_int_distribution<int> global_int_distribution(0, upcxx::world().rank_n()-1);
    uniform_int_distribution<int> local_int_distribution(0, n_threads_per_machines-1);
    uniform_real_distribution<double> real_distribution(0.0,1.0/sqrt(k));
    JobQueueList job_queues(n_threads_per_machines);
    tbb::concurrent_queue<ColumnData> send_queue;

    for (int j=0; j<(n/upcxx::world().rank_n()); j++){  /// ASSUME THAT n/upcxx::world().rank_n() is even
        std::vector<double> h_j(k);
        for(int l=0; l<k; l++){
            h_j[l] = real_distribution(generator); 
        }
        // randomize a worker containing h_i
        int randomized_thread_id = local_int_distribution(generator);
        ColumnData column_data;
        column_data.item_index = j;
        column_data.perm_index = 0;
        column_data.values = h_j;
        job_queues.push_local(randomized_thread_id, column_data); 
    }
    // local_perm for circulating item pairs around local threads.
    std::vector<int> local_perm_;
    local_perm_.resize(n_threads_per_machines * n_retries);
    for (int i=0; i<n_retries; i++){
        for (int j=0; j<n_threads_per_machines; j++){
            int randomized_thread_id = local_int_distribution(generator);
            local_perm_[i*n_threads_per_machines + j] = randomized_thread_id;
        }
    }
    std::vector<RatingHashMap> A_list;
    A_list.resize(n_threads_per_machines);
    std::function<void(int)> read_dataset_func = [&](int thread_id)->void{
        /*
        Netflix: /home/hpcc/cloud/nomad/netflix_prize/netflix_data_  + ... + .txt
        ml-20m: /home/hpcc/cloud/nomad/ml-20m/ratings_ + ... + .txt (csv)
        ml-10m: /home/hpcc/cloud/nomad/ml-10m/ratings_ + ... + .txt
        ml-100k: /home/hpcc/cloud/nomad/ml-100k/u1_ + ... + .base
        */
        const std::string train_dataset_path = "/home/hpcc/cloud/nomad/ml-10m/ratings_" + std::to_string(upcxx::rank_me() * n_threads_per_machines + thread_id) + ".txt";
        fstream newfile;
        newfile.open(train_dataset_path, ios::in); //open a file to perform read operation using file object
        if (newfile.is_open()){ //checking whether the file is open
            string tp;
            while(getline(newfile, tp)){ //read data from file object and put it into string.
                string line_tk;
                int e_idx = 0;
                std::stringstream stream_tp(tp);    
                int user_index, item_index;
                double rating;
                while(getline(stream_tp, line_tk, delimiter)){
                    switch (e_idx) {
                        case 0:{
                            user_index = stoi(line_tk) - 1;
                            break; 
                        } 
                        case 1:{
                            item_index = stoi(line_tk) - 1;
                            break;
                        }
                        case 2:{
                            // stoi for ml-10k, netflix, ml-10m, and stod for ml-20m
                            rating = stoi(line_tk) * 1.0/5.0; 
                            break;
                        }   
                        default:{
                            break;
                        }   
                    }
                    e_idx++;
                }
                //4 for ml-10k, ml-20m and 
                //3 for ml-10m and netflix
                if (e_idx == 3){     
                    A_list[thread_id].insert(user_index, item_index, rating);
                }
            }
            newfile.close();
        }
    };

    std::function<void(int)> run_train_func = [&](int thread_id)->void { 
        // Initialize parameters
        std::vector<std::vector<double>> l_w;
        l_w.resize(block_size);
        for(int i=0; i<block_size; i++){
            std::vector<double> temp;
            for (int j=0; j<k; j++){
                temp.push_back(real_distribution(generator));
            }
            l_w[i] = temp;
        }
        int num_updates = 0;
        while ((*stop_signal) == false){
            ColumnData item_info;
            while(job_queues.try_pop(thread_id, item_info)){
                int j = item_info.item_index;   
                std::vector<double> h_j = item_info.values;   /// vector size: (k, 1)
                int item_perm_index = item_info.perm_index;

                std::unordered_map<int, std::pair<double, int>> Aj = A_list[thread_id].get_by_item(j);
                double current_square_loss = 0.0;
                long current_loss_count = 0;
                for(auto it : Aj){
                    int i = it.first % block_size;   
                    double Aij = it.second.first; ////     scalar  
                    int t = it.second.second;
                    A_list[thread_id].increase_num_updates(it.first, j);
                    double step_size = learning_rate * 1.5 /
                            (1.0 + decay_rate * pow(t  + 1, 1.5)); // this is different from the source code of authors.
                        // note: have to index from global user_index (i) -> local user_index (i%block_size)
                    std::vector<double> w_i = l_w[i]; // vector size: (k, 1); 
                    double cur_loss = w_i * h_j - Aij;//w_i * h_j - Aij;

                    l_w[i] = w_i - step_size * (cur_loss * h_j + lambda * w_i);
                    h_j = h_j - step_size * (cur_loss * w_i + lambda * h_j);

                    current_square_loss += pow(cur_loss, 2);
                    current_loss_count += 1;
                }

                std::tuple<double, long, long> loss_tuple(
                    current_square_loss,
                    current_loss_count,
                    num_updates
                );
                (*distributed_losses)[thread_id].push_back(loss_tuple);
                num_updates += 1;
                int next_q=-1;
                ColumnData column_data;
                column_data.item_index = j;
                column_data.values = h_j;
                column_data.perm_index = 0;
                if (upcxx::world().rank_n() != upcxx::local_team().rank_n()){
                    if (item_perm_index >= n_retries * n_threads_per_machines){ // send to other machines
                        send_queue.push(column_data); 
                        buffer_count++;
                    } else{
                        while (true){
                            next_q = local_perm_[item_perm_index];
                            if (next_q != thread_id || item_perm_index >= n_retries * n_threads_per_machines) break;
                            item_perm_index += 1;
                        }
                        if (item_perm_index >= n_retries * n_threads_per_machines && next_q == 0){
                            send_queue.push(column_data); 
                            buffer_count++;
                        } else {
                            column_data.perm_index = item_perm_index + 1;   
                            job_queues.push_local(next_q, column_data);
                        }
                    }
                } else{
                    next_q = local_int_distribution(generator);
                    while (true){
                        if (next_q != thread_id) break;
                        next_q = local_int_distribution(generator);
                    }
                    job_queues.push_local(next_q, column_data);
                }
                if(buffer_count % UNITS_PER_MSG == 0){             
                    sending_signal = true;
                }
                (*training_steps)[thread_id] = num_updates; 
            } 
        }
    };
    for (int tid=0; tid<n_threads_per_machines; tid++) {
        computing_threads[tid] = new thread(read_dataset_func, tid);
    }
    for (int tid=0; tid<n_threads_per_machines; tid++) {
        computing_threads[tid]->join();
    }

    upcxx::barrier(); // Make sure all workers have read the dataset.
    cout << "WORKER " << upcxx::rank_me() << " 'VE BEEN DONE READING DATASET...\n";
    // Start the computing thread (run the training stage)
    for (int tid=0; tid<n_threads_per_machines; tid++) {
        computing_threads[tid] = new thread(run_train_func, tid);
    }

    // Define the sending thread
    thread sending_thread( [&]() {
        while ((*stop_signal) == false){
            int n_pop_tries = 0;
            while(sending_signal) { // training threads are waiting...
                ColumnData item_info;
                if(send_queue.try_pop(item_info)){
                    int retries = 0;
                    int next_rank = -1;
                    while(true){
                        next_rank = global_int_distribution(generator);
                        if (next_rank != upcxx::rank_me()) break;
                        if(retries >= 50){
                            next_rank = -1;  
                            break;
                        }
                        retries++;
                    }
                    if (next_rank >= 0){
                        int randomized_thread_id = local_int_distribution(generator);
                        job_queues.push_remote(next_rank, randomized_thread_id, item_info);
                    }
                } else{
                    sched_yield();
                    upcxx::progress();
                    n_pop_tries++;
                }
                if (n_pop_tries > 50){
                    sending_signal = false;
                }
            }
        }
    });

    // Define the receiving thread
    upcxx::liberate_master_persona();
    thread receiving_thread( [&]() {
        upcxx::persona_scope scope(upcxx::master_persona());
        auto start = std::chrono::system_clock::now();
        int delta_t = 5;
        while ((*stop_signal)==false){
            sched_yield();
            upcxx::progress();
            auto end = std::chrono::system_clock::now();
            std::chrono::duration<double> elapsed_seconds = end-start;
            if (upcxx::rank_me() == 0 && (int) rint(elapsed_seconds.count()) % delta_t == 0){ // only check each 5 seconds
                // pull training_steps
                int need_stop = true;
                int current_steps = 0;
                int the_lowest = 10000000;
                for(int i=1; i<upcxx::rank_n(); i++){
                    std::vector<int> pulled_training_steps = training_steps.fetch(i).wait();
                    for(auto training_steps : pulled_training_steps){
                        current_steps += training_steps;
                        if(training_steps < MAX_UPDATES/(upcxx::rank_n() * n_threads_per_machines)){
                            need_stop = false;
                            the_lowest = min(the_lowest, training_steps);
                        }
                    }
                }
                if (current_steps < MAX_UPDATES){
                    cout << "Training step: [" << current_steps << "/" << MAX_UPDATES << "]\n";
                    cout.flush();
                }
                if (need_stop){
                    (*stop_signal) = true;
                    for(int i=1; i<upcxx::rank_n(); i++){
                        upcxx::rpc(i, [](upcxx::dist_object<bool> &d_signal){
                            *(d_signal) = true;
                        }, stop_signal).wait();  
                    }
                } else{
                    if(current_steps > MAX_UPDATES){
                        double percentage = the_lowest * 100.0 /(MAX_UPDATES/(upcxx::rank_n()*n_threads_per_machines));
                        cout << "Waiting for the slowest worker loading " << percentage << "%...\n";
                        cout.flush();
                        delta_t = 20;
                    } 
                }
            }

        }
    });
            
    // thread
    for (int tid=0; tid<n_threads_per_machines; tid++) {
        computing_threads[tid]->join();
    }
            
    sending_thread.join();
    receiving_thread.join();
    upcxx::persona_scope scope(upcxx::master_persona());
    upcxx::barrier();
    // Fetching distributed lossess, accumulating, and storing them in a final output file.
    if(upcxx::rank_me() == 0){
        int total_count = 0;
        double total_loss = 0.0;
        int t = 0;
        std::vector<std::pair<double,long>> accumulated_losses;
        accumulated_losses.resize(MAX_UPDATES);
        for(int i=0; i<upcxx::rank_n(); i++){
            std::vector<std::vector<std::tuple<double, long, long>>> tmp_list = distributed_losses.fetch(i).wait();
            for(int j=0; j<n_threads_per_machines; j++){
                std::vector<std::tuple<double, long, long>> loss_tuple_list = tmp_list[j];
                for(std::tuple<double, long, long> loss_tuple : loss_tuple_list){
                    int update_idx = std::get<2>(loss_tuple);
                    update_idx = update_idx * (upcxx::rank_n() * n_threads_per_machines) + j * upcxx::rank_n() + i;
                    if (update_idx >= MAX_UPDATES){
                        continue;
                    }
                    accumulated_losses[update_idx].second += std::get<1>(loss_tuple);
                    accumulated_losses[update_idx].first += std::get<0>(loss_tuple);
                }
            }
        }
        // Compute squared losses
        int step_idx = 0;
        std::ofstream outfile;
        outfile.open("result.txt", std::ios_base::app);
        double accumulated_loss = 0.0;
        long accumulate_loss_count = 0;
        for(std::pair<double, long> loss_info: accumulated_losses){
            accumulated_loss += loss_info.first;
            accumulate_loss_count += loss_info.second;
            double square_loss = (accumulate_loss_count > 0) ? sqrt(accumulated_loss/accumulate_loss_count) : 0;
            outfile << step_idx << "\t" << square_loss << "\n";
            step_idx++;
        }
        cout << "DONE!\n";
    }
    upcxx::finalize();
    return 0;
}
