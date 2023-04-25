# Problem Description
# Given n non-negative integers A[0], A[1], ..., A[n-1] , where each represents a point at coordinate (i, A[i]).
# N vertical lines are drawn such that the two endpoints of line i is at (i, A[i]) and (i, 0).
# Find two lines, which together with x-axis forms a container, such that the container contains the most water.
# Note: You may not slant the container.

# Problem Constraints
# 0 <= N <= 105
# 1 <= A[i] <= 105

# Input Format
# Single Argument representing a 1-D array A.

# Output Format
# Return single Integer denoting the maximum area you can obtain.


# Example Input
# Input 1:
# A = [1, 5, 4, 3]

# Input 2:
# A = [1]


# Example Output
# Output 1:
#  6

# Output 2:
#  0


# Example Explanation
# Explanation 1:
# 5 and 3 are distance 2 apart. So size of the base = 2. Height of container = min(5, 3) = 3. 
# So total area = 3 * 2 = 6

# Explanation 2:
# No container is formed.

class Solution:
    # @param A : list of integers
    # @return an integer
    def maxArea(self, A):
        left, right, n = 0, len(A)-1, len(A)-1
        if n <= 1:
            return 0
        
        max_water = 0
        while left <= right:
            max_water = max(min(A[left], A[right])*(right-left), max_water)

            if A[left] > A[right]:
                right -= 1
            elif A[left] < A[right]:
                left += 1
            else:
                right -= 1
                left += 1
        return max_water
            
            


        