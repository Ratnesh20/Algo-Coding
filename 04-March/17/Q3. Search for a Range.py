class Solution:
    # @param A : tuple of integers
    # @param B : integer
    # @return a list of integers
    def searchRange(self, A, B):
        n = len(A)
        left, right, mid = 0, n-1, (n-1)//2
        start = 0
        end = n
        while left <= right:
            if A[mid] == B and (mid == 0 or A[mid] < A[mid-1]):
                start = mid
        
            if A[mid] <= B:
                left = mid + 1
            else:
                right = mid - 1
            mid = (left+ right)//2
        
        start = mid if A[mid] == B else -1


        left, right, mid = 0, n-1, (n-1)//2
        while left <= right:
            if A[mid] == B and (mid == n-1 or A[mid] < A[mid+1]):
                start = mid
        
            if A[mid] <= B:
                left = mid + 1
            else:
                right = mid - 1
            mid = (left+ right)//2
        
        end = mid if A[mid] == B  else -1

        return [start, end]
    

class Solution:
    # @param A : tuple of integers
    # @param B : integer
    # @return a list of integers
    def searchRange(self, A, B):
        n = len(A)
        start, end = -1, -1
        
        # Find the leftmost occurrence of B
        low, high = 0, n-1
        while low <= high:
            mid = (low + high) // 2
            if A[mid] < B:
                low = mid + 1
            else:
                high = mid - 1
                if A[mid] == B:
                    start = mid
        
        # Find the rightmost occurrence of B
        low, high = 0, n-1
        while low <= high:
            mid = (low + high) // 2
            if A[mid] <= B:
                low = mid + 1
                if A[mid] == B:
                    end = mid
            else:
                high = mid - 1
        
        return [start, end]
