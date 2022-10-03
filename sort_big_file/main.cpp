//
//  main.cpp
//  sort_big_file
//
//  Created by Павел
//

#include <iostream>
#include <queue>
#include <thread>
#include <mutex>
#include <vector>
#include <condition_variable>
#include <fstream>
#include <cstdlib>
#include <stdio.h>
#include <algorithm>
#include <functional>
#include <numeric>
#include <iomanip>
#include <string>
#include <ctime>

using namespace std;

int count_file = 0;
const int n = 32*1024*1024; //268435456;
queue <string> filiki;

string hy;
vector <int> workers = { false, false, false, false };
mutex mut;
condition_variable c;

bool do_file(ifstream& f, string& name, vector <int>& part_of_file){
    unique_lock <mutex> lk(mut);
    if (f.eof()){
        return false;
    }
    count_file += 1;
    int a;
    for (int i = 0; i < n; i++){
        f.read(reinterpret_cast<char*>(&a), sizeof(int));
        if (f.eof()){
            return false;
        }
        part_of_file[i] = a;
    }
    name = to_string(count_file);
    return true;
}

void file_open(ifstream& f, int nomer_thread){
    while (true){
        string name;
        vector <int> new_file(n);
        cout << "OK" << endl;
        bool go = do_file(f, name, new_file);
        if (!go){
            break;
        }
        sort(new_file.begin(), new_file.end());
        filiki.push(name);
        ofstream fi(name, ios::binary);
        for (int i = 0; i < new_file.size(); i++){
            fi.write(reinterpret_cast<char*>(&new_file[i]), sizeof(int));
        }
        
        fi.close();
        ifstream y(name, ios::binary);
        y.close();
    }
}

bool is_working(){
    return accumulate(workers.begin(), workers.end(), false);
}

bool do_merge(string& name1, string& name2, int nomber_thred)
{
    unique_lock <mutex> lk(mut);
    c.wait(lk, [] {return filiki.size() > 1 || !is_working(); });
    if (filiki.size() == 1 || filiki.size() == 0){
        return false;
    }
    name1 = filiki.front();
    filiki.pop();
    
    name2 = filiki.front();
    filiki.pop();
    
    hy = name1 + "1";
    workers[nomber_thred - 1] = true;
    return true;
}

void merge_file(int nomber_thread){
    while (true){
        string name;
        string name1;
        string name2;
        
        int a, b;
        if (!do_merge(name1, name2, nomber_thread)){
            break;
        }
        name = name1 + "1";
        ifstream f(name1, ios::binary);
        ifstream fi(name2, ios::binary);
        
        f.read(reinterpret_cast<char*>(&a), sizeof(int));
        fi.read(reinterpret_cast<char*>(&b), sizeof(int));
        
        ofstream g(name, ios::binary);
        
        while (!f.eof() && !fi.eof()){
            if (a > b){
                g.write(reinterpret_cast<char*>(&b), sizeof(int));
                fi.read(reinterpret_cast<char*>(&b), sizeof(int));
                continue;
            } else {
                g.write(reinterpret_cast<char*>(&a), sizeof(int));
                f.read(reinterpret_cast<char*>(&a), sizeof(int));
                continue;
            }
        }
        
        if (f.peek() == EOF && fi.peek() == EOF){
            if (a > b){
                g.write(reinterpret_cast<char*>(&a), sizeof(int));
            } else {
                g.write(reinterpret_cast<char*>(&b), sizeof(int));
            }
        }
        
        if (f.peek() == EOF && fi.peek() != EOF){
            while (fi.peek()!= EOF){
                g.write(reinterpret_cast<char*>(&b), sizeof(int));
                fi.read(reinterpret_cast<char*>(&b), sizeof(int));
            }
            g.write(reinterpret_cast<char*>(&b), sizeof(int));
        }
        
        if (fi.peek() == EOF && f.peek() != EOF){
            while (f.peek() != EOF){
                g.write(reinterpret_cast<char*>(&a), sizeof(int));
                
                f.read(reinterpret_cast<char*>(&a), sizeof(int));
            }
            g.write(reinterpret_cast<char*>(&a), sizeof(int));
        }
        f.close();
        fi.close();
        g.close();
        
        remove(name1.c_str());
        remove(name2.c_str());
        
        lock_guard <mutex> lg(mut);
        filiki.push(name);
        workers[nomber_thread - 1] = false;
        c.notify_all();
    }
    workers[nomber_thread - 1] = false;
}

int main(int argc, char* argv[]){
    int a;
    ofstream fo("/Users/pavel/Desktop/sort_big_file/data/data_1.bin", ios::binary);
    for (int i = 0; i < n*8 + 109; i++){
        a = 1 + rand() % 100;
        fo.write(reinterpret_cast<char*>(&a), sizeof(int));
    }
    cout << endl;
    fo.close();
    
    
    ifstream f("/Users/pavel/Desktop/sort_big_file/data/data_1.bin", ios::binary);
    
    thread t1(file_open, ref(f), 1);
    thread t2(file_open, ref(f), 2);
    thread t3(file_open, ref(f), 3);
    thread t4(file_open, ref(f), 4);
    
    t1.join();
    t2.join();
    t3.join();
    t4.join();
    
    f.close();
    
    thread h1(merge_file, 1);
    thread h2(merge_file, 2);
    thread h3(merge_file, 3);
    thread h4(merge_file, 4);
    
    h1.join();
    h2.join();
    h3.join();
    h4.join();
    
    ifstream hh(hy, ios::binary);
    
    cout << "answer" << endl;
    
    int k = 0;
    int count_f = 0;
    
    while (hh.peek() != EOF){
        hh.read(reinterpret_cast<char*>(&k), sizeof(int));
        cout << k << endl;
        count_f++;
    }
    hh.close();
    remove(hy.c_str());
    
    cout << n*8 << endl;
    return 0;
    
}
