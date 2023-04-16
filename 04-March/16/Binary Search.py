class Solution:
    # @param A : list of integers
    # @param B : integer
    # @return an integer
    def searchInsert(self, A, B):
        left, right, mid = 0, len(A)-1, (len(A)-1)//2
        while left < right:
            if A[mid] == B:
                return mid
        
            if A[mid] <= B:
                left = mid + 1
            else:
                right = mid - 1
            mid = (left+ right)//2
        
        return mid if A[mid] == B or A[mid] >= B else mid + 1