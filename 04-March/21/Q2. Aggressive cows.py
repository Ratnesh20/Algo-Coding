# Problem Description
# Farmer John has built a new long barn with N stalls. Given an array of integers A of size N where each element of the array represents the location of the stall and an integer B which represents the number of cows.
# His cows don't like this barn layout and become aggressive towards each other once put into a stall. To prevent the cows from hurting each other, John wants to assign the cows to the stalls, such that the minimum distance between any two of them is as large as possible. What is the largest minimum distance?

# Problem Constraints
# 2 <= N <= 100000
# 0 <= A[i] <= 109
# 2 <= B <= N

# Input Format
# The first argument given is the integer array A.
# The second argument given is the integer B.

# Output Format
# Return the largest minimum distance possible among the cows.

# Example Input
# Input 1:

# A = [1, 2, 3, 4, 5]
# B = 3
# Input 2:

# A = [1, 2]
# B = 2

# Example Output
# Output 1:
#  2
# Output 2:
#  1


# Example Explanation
# Explanation 1:
#  John can assign the stalls at location 1, 3 and 5 to the 3 cows respectively. So the minimum distance will be 2.

# Explanation 2:
#  The minimum distance will be 1.

class Solution:
    # @param A : list of integers
    # @param B : integer
    # @return an integer
    def solve(self, A, B):

        def insert_cow(A, min_dis, B):
            last_cow = 0
            B -= 1
            for i in range(1,len(A)):
                if i-last_cow >= min_dis:
                    last_cow = i
                    B -= 1
                if B == 0:
                    return True
            if B > 0:
                return False
            

                

        n = len(A)
        min_dis, left, right = -1, A[1]-A[0], A[n-1] - A[0]
        
        for i in range(2, n):
            if A[i] - A[i-1] < left:
                left = A[i] - A[i-1]
        
        while left < right:
            mid = (left+right)//2
            
            res = insert_cow(A, mid, B)
            if res:
                min_dis = mid
                left = mid + 1
            else:
                right = mid - 1
        return min_dis


