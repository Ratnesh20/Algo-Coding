#include<iostream>
#include<vector>
using namespace std;

class Solution {
public:
    int isWinner(vector<int>& player1, vector<int>& player2) {
        const int n = player1.size();
        int flagA = 1, flagB = 1, pl1 = 0, pl2 = 0;
   
        for (int i = 0; i < n; i++){
            if (player1[i] == 10) flagA = 2;
            if (player2[i] == 10) flagB = 2;
            
            pl1 += flagA*player1[i];
            pl2 += flagB*player2[i];
        }  
        if (pl1 == pl2){
            return 0;
        }
        else if (pl1 > pl2){
            return 1;
        }
        else {
            return 0;
        }
    }
};


int main()
{
    vector<int> g1;
 
    for (int i = 1; i <= 5; i++)
        g1.push_back(i);
 
    cout << "Output of begin and end: ";
    for (auto i = g1.begin(); i != g1.end(); ++i)
        cout << *i << " ";
 
    cout << "\nOutput of cbegin and cend: ";
    for (auto i = g1.cbegin(); i != g1.cend(); ++i)
        cout << *i << " ";
 
    cout << "\nOutput of rbegin and rend: ";
    for (auto ir = g1.rbegin(); ir != g1.rend(); ++ir)
        cout << *ir << " ";
 
    cout << "\nOutput of crbegin and crend : ";
    for (auto ir = g1.crbegin(); ir != g1.crend(); ++ir)
        cout << *ir << " ";
 
    return 0;
}