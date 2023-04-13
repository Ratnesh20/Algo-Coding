# Problem Description
# Given an array of integers A, find and return the minimum value of | A [ i ] - A [ j ] | where i != j and |x| denotes the absolute value of x.

# Problem Constraints
# 2 <= length of the array <= 100000
# -109 <= A[i] <= 109

# Input Format
# The only argument given is the integer array A.

# Output Format
# Return the minimum value of | A[i] - A[j] |.


# Example Input
# Input 1:
#  A = [1, 2, 3, 4, 5]

# Input 2:
#  A = [5, 17, 100, 11]

# Example Output
# Output 1:
#  1

# Output 2:
#  6


# Example Explanation
# Explanation 1:
#  The absolute difference between any two adjacent elements is 1, which is the minimum value.
# Explanation 2:
#  The minimum value is 6 (|11 - 5| or |17 - 11|).

class Solution:
    def solve(self, A):
        res = self.mergesort(A)
        min_ = float('inf')
        for i in range(1,len(res)):
            min_ = min(min_, abs(res[i-1]-res[i]))
        return min_
    
    def mergesort(self, A):
        if len(A) <= 1:
            return A
        
        mid = (len(A))//2
        left  = self.mergesort(A[:mid])
        right = self.mergesort(A[mid:])

        return self.merge(left, right)
        
    
    def merge(self, A, B):
        n,m,i,j,k = len(A), len(B), 0, 0, 0
        res = [0]*(m+n)
        while i < n and j < m:
            if A[i] <= B[j]:
                res[k] = A[i]
                i += 1
            else:
                res[k] = B[j]
                j += 1
            k += 1

        while i < n:
            res[k] = A[i]
            i += 1
            k += 1
        
        while j < m:
            res[k] = B[j]
            j += 1
            k += 1
        
        return res
