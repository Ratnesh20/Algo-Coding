# Problem Description
# Given a sorted array of integers A of size N and an integer B.
# array A is rotated at some pivot unknown to you beforehand.
# (i.e., 0 1 2 4 5 6 7 might become 4 5 6 7 0 1 2 ).
# You are given a target value B to search. If found in the array, return its index otherwise, return -1.
# You may assume no duplicate exists in the array.
# NOTE: Users are expected to solve this in O(log(N)) time.

# Problem Constraints
# 1 <= N <= 1000000
# 1 <= A[i] <= 109
# all elements in A are distinct.

# Input Format
# The first argument given is the integer array A.

# The second argument given is the integer B.

# Output Format
# Return index of B in array A, otherwise return -1

# Example Input
# Input 1:
# A = [4, 5, 6, 7, 0, 1, 2, 3]
# B = 4 

# Input 2:
# A : [ 9, 10, 3, 5, 6, 8 ]
# B : 5

# Example Output
# Output 1:
#  0 
# Output 2:
#  3

# Example Explanation
# Explanation 1:
# Target 4 is found at index 0 in A. 

# Explanation 2:
# Target 5 is found at index 3 in A.

# Not Working
class Solution:
    # @param A : tuple of integers
    # @param B : integer
    # @return a list of integers
    def search(self, A, B):
        n = len(A)
        
        x = A[0]
        low, high = 0, n-1
        while low <= high:
            mid = (low + high) // 2
            if A[mid] > x:
                low = mid + 1
            else:
                high = mid - 1
        
        A, mido = (A[mid:], mid) if B < x else (A[:mid+1], 0)

        
        # Find the leftmost occurrence of B
        low, high = 0, len(A)-1
        while low <= high:
            mid = (low + high) // 2
            if A[mid] < B:
                low = mid + 1
            else:
                high = mid - 1
                # Keep Updating till the low > high
                if A[mid] == B:
                    return mido+mid
        return -1
                

#  GPT
def search_rotated_array(A, B):
    low, high = 0, len(A)-1
    
    while low <= high:
        mid = (low + high) // 2
        
        if A[mid] == B:
            return mid
        
        if A[mid] >= A[low]:
            if B >= A[low] and B < A[mid]:
                high = mid - 1
            else:
                low = mid + 1
        else:
            if B > A[mid] and B <= A[high]:
                low = mid + 1
            else:
                high = mid - 1
    
    return -1
