class Solution:
    # @param A : integer
    # @return an integer
    def sqrt(self, A):
        if A <= 1: return A
        left, right = 1, A

        while left <= right:
            mid = (left+right)//2
            if mid**2 == A:
                return mid
            elif mid**2 <= A:
                left = mid+1
            else:
                right = mid-1
        return (left+right)//2