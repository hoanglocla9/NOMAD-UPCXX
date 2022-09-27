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
    #include <time.h>
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

    class DistributedQueue{       
        private:
            using item_queue = upcxx::dist_object<tbb::concurrent_queue<ColumnData>>; // 
            item_queue local_queue;

        public:    
            DistributedQueue () : local_queue({}){};
            upcxx::future<> push_item(const int &q_index, const ColumnData & column_data ){ // const int &item_index, const std::vector<double> &item_vector 
                return upcxx::rpc(q_index, [](item_queue &l_queue, const ColumnData & column_data) // 
                    {
                        l_queue->push(column_data);
                    }, local_queue, column_data); //       
            }
            bool isEmpty(){
                return (*local_queue).empty();        
            }
            bool try_pop(ColumnData &element){
                return (*local_queue).try_pop(element);    
            }
    };


    class DistributedHashMap {       
        private:
            using d_map = upcxx::dist_object<std::unordered_map<int, std::unordered_map<int, std::pair<double, int>>>>; // store <item_id, <user_id, <rating, t>>>>
            d_map local_map;

            int get_target_rank(const int &user_index) {
                return 1 + (user_index % (upcxx::world().rank_n()-1));
            }
        public:    
            DistributedHashMap () : local_map({}){};
            upcxx::future<> insert_remote(const int &user_index, const int &item_index, const double &rating){ // 
                return upcxx::rpc(get_target_rank(user_index), [](d_map &l_map, const int & user_idx, const int &item_idx, const double &rating) // 
                    {
                       auto it = l_map->find(item_idx);
                       if (it == l_map->end()){	
                            std::unordered_map<int, std::pair<double, int>> new_entry{};
                            std::pair<double, int> rating_and_update;
                            rating_and_update.first = rating;
                            rating_and_update.second = 0;
                            new_entry.insert({user_idx, rating_and_update});
                            l_map->insert({item_idx, new_entry});
                       } else{
                            std::unordered_map<int, std::pair<double, int>> old_entry = it->second;
                            std::pair<double, int> rating_and_update;
                            rating_and_update.first = rating;
                            rating_and_update.second = 0;
                            it->second.insert({user_idx, rating_and_update});
                       }                   
                    }, local_map, user_index, item_index, rating); //       
            }

            std::unordered_map<int, std::pair<double, int>> get_by_item(const int &item_index){
                auto it = local_map->find(item_index);
                if (it == local_map->end()){
                    std::unordered_map<int, std::pair<double, int>> empty_entry{};
                    return empty_entry;
                }
                return it->second;
            }
            void print_map(){
                for(int i=0; i<upcxx::rank_n(); i++){
                    upcxx::rpc(i, [](d_map &l_map){
                        int count = 0;
                        for(auto it: *l_map){
                            for(auto it2: it.second){
                                count += 1;
                            }
                        }
                        cout << "Rank " << upcxx::rank_me() << " has: " << count << "\n";
                    }, local_map).wait();    
                }
            }

            void increase_num_updates(const int &user_idx, const int &item_idx){
                auto it = local_map->find(item_idx);
                if (it == local_map->end()){    
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


    bool test_conditions(std::vector<double>w_i, std::vector<double>h_j){
        bool result = false;
        for(int i =0; i<w_i.size(); i++){
            result = result || abs(w_i[i])>10 || abs(h_j[i])>10;
        }
        return result;
    }

    int main(int argc, char **argv) {
        clock_t start, end;
        start = clock();
        upcxx::init();
        const double lambda = 0.05; // regularization
        const double decay_rate = 0.012;
        const double learning_rate = 0.001;
        const int n_retries = 1; // number of circulating a (i, j) in a machine
        const double epsilon = 0.0000001; // stop threshold
        const int MAX_UPDATES = 300000/(upcxx::rank_n()-1);
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
        int m = 943;
        int n = 1682;
        int k = 20;
        char delimiter = '\t';

        // for ml-10m
        // int m = 71567;
        // int n = 10681;
        // int k = 100;
        // char delimiter = ','; 

        int block_size = m/(upcxx::world().rank_n()-1);
        int n_local_members = upcxx::local_team().rank_n();
        if(upcxx::local_team_contains(0)){ // local team contains root node ?
            n_local_members = n_local_members-1;
        }

        // const std::string train_dataset_path = "/home/hpcc/cloud/nomad/netflix_prize/netflix_data_" + std::to_string(upcxx::rank_me()) + ".txt";
    //    const std::string train_dataset_path = "/home/hpcc/cloud/nomad/ml-20m/ratings_" + std::to_string(upcxx::rank_me()) + ".csv";
       const std::string train_dataset_path = "/home/hpcc/cloud/nomad/ml-100k/u1_" + std::to_string(upcxx::rank_me()) + ".base";
        // const std::string train_dataset_path = "/home/hpcc/cloud/nomad/ml-10m/ratings_" + std::to_string(upcxx::rank_me()) + ".txt";

      //  const std::string train_dataset_path = "/home/picarib/Downloads/NOMAD-UPCXX/ml-20m/ratings_" + std::to_string(upcxx::rank_me()) + ".csv";
       // const std::string train_dataset_path = "/home/picarib/Downloads/NOMAD-UPCXX/ml-100k/u1_" + std::to_string(upcxx::rank_me()) + ".base";
        // const std::string train_dataset_path = "/home/picarib/Downloads/nomad/netflix_prize/netflix_data_" + std::to_string(upcxx::rank_me()) + ".txt";
        //const std::string train_dataset_path = "/home/picarib/Downloads/NOMAD-UPCXX/ml-10m/ratings_" + std::to_string(upcxx::rank_me()) + ".txt";

        default_random_engine generator;
        uniform_real_distribution<double> real_distribution(0.0,1.0/sqrt(k));
        uniform_int_distribution<int> global_int_distribution(1, upcxx::world().rank_n()-1);
        DistributedHashMap A;
        DistributedHashMap A_test;

        // Initialize parameters
        std::vector<std::vector<double>> l_w;
        if(upcxx::rank_me() != 0){
            l_w.resize(block_size);
            for(int i=0; i<block_size; i++){
                std::vector<double> temp;
                for (int j=0; j<k; j++){
                    temp.push_back(real_distribution(generator));
                }
                l_w[i] = temp;
            }
        }
        DistributedQueue d_queue;
        if (upcxx::rank_me() == 0){
            cout << "START INIT AT ROOT NODE \n";
            for (int j=0; j<n; j++){
                // init h_j
                std::vector<double> h_j(k);
                for(int l=0; l<k; l++){
                    h_j[l] = real_distribution(generator);                 
                }
                // randomize a worker containing h_i
                int randomized_q = global_int_distribution(generator);
                ColumnData column_data;
                column_data.item_index = j;
                column_data.perm_index = 0;
                column_data.values = h_j;
                d_queue.push_item(randomized_q, column_data).wait(); 
            }
        }
            // read train dataset to build the matrix A.
        cout << "  WORKER " <<  upcxx::rank_me() << " IS READING THE TRAINING DATASET... \n";
        fstream newfile;
        newfile.open(train_dataset_path, ios::in); //open a file to perform read operation using file object

        upcxx::future<> fut_full = upcxx::make_future();
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
                            rating = stoi(line_tk) * 1.0/5.0; // stoi for ml-10k, netflix, ml-10m, and stod for ml-20m
                            break;
                        }	
                        default:{
                            break;                        
                        }	
                    }
                    e_idx++;		
                }
                if (e_idx == 4){ //4 for ml-10k and 3 for others    
                    upcxx::future<> fut = A.insert_remote(user_index, item_index, rating);
                    fut_full = upcxx::when_all(fut_full, fut);
                }
            }
            newfile.close();
            fut_full.wait();
        }
        upcxx::barrier();

        if(upcxx::rank_me() == 0){
            A.print_map();
        }
        
        // Init for permature at local node
        upcxx::global_ptr<double> perm_; 
        if (upcxx::local_team().rank_me() == 0){ // create perm_ list at root process of a machine.
            perm_ = upcxx::new_array<double>(n_retries * n_local_members);
        }
        perm_ = upcxx::broadcast(perm_, 0, upcxx::local_team()).wait();
        double * local_perm_ = perm_.local(); 

        upcxx::global_ptr<int> local_rank_of_root_node;
        if(upcxx::world().rank_me() == 0){ // get local rank of root node
            local_rank_of_root_node = upcxx::new_<int>(upcxx::local_team().rank_me());
        }
        local_rank_of_root_node = upcxx::broadcast(local_rank_of_root_node, 0, upcxx::local_team()).wait();

        if (upcxx::local_team().rank_me() == 0){
            for (int i=0; i<n_retries; i++){
                for (int j=0; j<n_local_members; j++){
                    int randomized_q = global_int_distribution(generator);
                    while(!upcxx::local_team_contains(randomized_q)){
                        randomized_q = global_int_distribution(generator);
                    }
                    local_perm_[i*n_local_members + j] = randomized_q;
                }
            }
        } 
        upcxx::barrier();
        // Init for global loss
        upcxx::dist_object<std::vector<std::tuple<double, long, long>>> distributed_losses({}); /// save <total loss and loss count>
        

        if(upcxx::world().rank_me() != 0){
            upcxx::future<> fut_batch = upcxx::make_future();
            int num_failures = 0;
            int num_updates = 0;
            int _t = 0;
            while (true){
                ColumnData item_info;
                if(d_queue.try_pop(item_info)){
                    int j = item_info.item_index;   
                    std::vector<double> h_j = item_info.values;   /// vector size: (k, 1)
                    int item_perm_index = item_info.perm_index;

                    std::unordered_map<int, std::pair<double, int>> Aj = A.get_by_item(j);
                    double current_square_loss = 0.0;
                    long current_loss_count = 0;
                    for(auto it : Aj){
                        int i = it.first % block_size;   
                        double Aij = it.second.first; ////     scalar  
                        int t = it.second.second;
                        _t = std::max(_t, t);//
                        A.increase_num_updates(it.first, j);
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
                    (*distributed_losses).push_back(loss_tuple);
                    int next_q=-1;
                    ColumnData column_data;
                    column_data.item_index = j;
                    column_data.values = h_j;
                    column_data.perm_index = 0;
                    if (upcxx::world().rank_n() != upcxx::local_team().rank_n()){
                        if (item_perm_index >= n_local_members * n_retries){ // send to other machines
                            int retries = 0;
                            while(true){
                                next_q = global_int_distribution(generator);
                                if (!upcxx::local_team_contains(next_q) && next_q != 0) break;
                                if(retries >= 50){
                                    next_q = -1;  
                                    break;
                                }
                                retries++;
                            }
                        } else{
                            while (true){
                                next_q = local_perm_[item_perm_index];
                                if (next_q != upcxx::rank_me()) break;
                                item_perm_index += 1;
                            }        
                            
                            if (next_q == 0){
                                int retries = 0;
                                while(true){
                                    next_q = global_int_distribution(generator);
                                    if (!upcxx::local_team_contains(next_q) && next_q != 0) break;
                                    if(retries >= 50){
                                        next_q = -1;  
                                        break;
                                    }
                                    retries++;
                                }
                            } else{
                                column_data.perm_index = item_perm_index + 1;    
                            }
                        }
                    } else{
                        while (true){
                            next_q = global_int_distribution(generator);
                            if (next_q != upcxx::rank_me()) break;
                        }
                        // column_data.perm_index = upcxx::rank_me(); 
                    }
                    if(next_q != -1){
                        // d_queue.push_item(next_q, column_data).wait();
                        upcxx::future<> fut = d_queue.push_item(next_q, column_data);
                        fut_batch = upcxx::when_all(fut_batch, fut);
                    }
                    if(num_updates >= MAX_UPDATES){
                        upcxx::progress();
                        break;
                    }
                    num_updates++;
                } else{
                    num_failures++;
                } 
                upcxx::progress();
                // if ((num_failures + num_updates)  % 10000 == 0){ // (num_failures + num_updates) % 10 == 0
                //     // fut_batch.wait();
                //     
                // }
            }
        } 

        upcxx::barrier();
        if(upcxx::rank_me() == 0){
            int total_count = 0;
            double total_loss = 0.0;
            int t = 0;
            std::vector<std::pair<double,long>> accumulated_losses;
            accumulated_losses.resize(MAX_UPDATES * (upcxx::rank_n()-1));
            for(int i=0; i<upcxx::rank_n()-1; i++){
                std::vector<std::tuple<double, long, long>> loss_tuple_list = distributed_losses.fetch(i+1).wait();
                for(std::tuple<double, long, long> loss_tuple : loss_tuple_list){
                    int update_idx = std::get<2>(loss_tuple); 
                    if (update_idx >= MAX_UPDATES){
                        continue;
                    }
                    accumulated_losses[update_idx * (upcxx::rank_n()-1) + i].second += std::get<1>(loss_tuple);
                    accumulated_losses[update_idx * (upcxx::rank_n()-1) + i].first += std::get<0>(loss_tuple);
                }
            }
            // Compute squared losses
            int step_idx = 0;
            std::ofstream outfile;
            outfile.open("result-v1.txt", std::ios_base::app);
            double accumulated_loss = 0.0;
            long accumulate_loss_count = 0;
            for(std::pair<double, long> loss_info: accumulated_losses){
                accumulated_loss += loss_info.first;
                accumulate_loss_count += loss_info.second;
                double square_loss = (accumulate_loss_count > 0) ? sqrt(accumulated_loss/accumulate_loss_count) : 0;
                outfile << step_idx << "\t" << square_loss << "\n";
                step_idx++;
            }
        }
        upcxx::finalize();
        cout << "TOTAL RUNNING TIME: " << double(end - start) / double(CLOCKS_PER_SEC) << "\n";
        return 0;
    }
