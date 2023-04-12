class Solution:
    # @param A : list of integers
     # @return an long
    def subarraySum(self, A):
        suma = 0
        N = len(A)
        for i in range(N):
            suma += (i+1)*(N-i)*A[i]
        
        return suma

s = Solution()
l = [2,9,5]
print(s.subarraySum(l))