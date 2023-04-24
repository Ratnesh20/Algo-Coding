class Solution:
    # @param A : tuple of integers
    # @return an integer
    def trap(self, A):
        water, n = 0, len(A)
        left_m, right_m = A[0], A[n-1]
        max_val, max_index = A[0], 0
        for i in range(n):
            if max_val < A[i]:
                max_val = A[i]
                max_index = i
        
        for i in range(1, max_index):
            left_m = max(left_m, A[i])
            water += left_m - A[i]
            

        for i in range(n-1-1,max_index, -1):
            right_m = max(right_m, A[i])
            water += right_m - A[i]
        
        return water