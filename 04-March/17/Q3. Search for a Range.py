# Problem Description
# Given a sorted array of integers A (0-indexed) of size N, find the starting and the ending position of a given integer B in array A.
# Your algorithm's runtime complexity must be in the order of O(log n).
# Return an array of size 2, such that the first element = starting position of B in A and the second element = ending position of B in A, if B is not found in A return [-1, -1].

# Problem Constraints
# 1 <= N <= 106
# 1 <= A[i], B <= 109

# Input Format
# The first argument given is the integer array A.
# The second argument given is the integer B.

# Output Format
# Return an array of size 2, such that the first element = starting position of B in A and the second element = the ending position of B in A. If B is not found in A return [-1, -1].

# Example Input
# Input 1:

#  A = [5, 7, 7, 8, 8, 10]
#  B = 8
# Input 2:

#  A = [5, 17, 100, 111]
#  B = 3


# Example Output
# Output 1:
#  [3, 4]
# Output 2:
#  [-1, -1]


# Example Explanation
# Explanation 1:
#  The first occurence of 8 in A is at index 3.
#  The second occurence of 8 in A is at index 4.
#  ans = [3, 4]

# Explanation 2:
#  There is no occurence of 3 in the array.


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
                # Keep Updating till the low > high
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
