class Solution:
    # @param A : list of integers
    # @param B : integer
    # @return an integer
    def solve(self, A, B):
        wsum = 0
        index = 0
        for i in range(B):
            wsum += A[i]
        
        minsum = wsum
        for i in range(1,len(A)-B+1):
            wsum = wsum - A[i-1] + A[B+i-1]
            AI = A[i]
            ABI = A[B+i-1]
            if minsum > wsum:
                minsum = wsum
                index = i
        return index

s = Solution()
print(s.solve([ 20, 3, 13, 5, 10, 14, 8, 5, 11, 9, 1, 11 ], 9)) #12
print(s.solve([3, 7, 90, 20, 10, 50, 40], 3)) #3
print(s.solve([3, 7, 5, 20, -10, 0, 12], 2))  #4