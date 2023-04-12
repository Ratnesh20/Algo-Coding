/**
 * @input A : Integer array
 * @input n1 : Integer array's ( A ) length
 * 
 * @Output Integer
 */
int solve(int* A, int n1) {
    int lsum = 0, rsum = 0, n = n1,i;

    for( i = 1; i<=n; i++){
        lsum += A[i];
    }
    for( i = 1; i<=n; i++){
        rsum += A[i-1];
        lsum -= A[i];

        if(lsum == rsum){
            return i;
        }
    }
    return -1;
}
