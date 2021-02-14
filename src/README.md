# Author: TOUNEKTI, Dhiaeddine
# My approach:

<p>
I used two optimization techniques the first one is to minimize the number of times when we do a join with large files.
For example if we have 3 files A, B, C ranked from the largest to the smallest, and we want to join them we first join 
C and B and then join A.
<br>
The second method is chain folding by combining mapper-only tasks and including them in the closest reducer, If none is
available then include them in the closest mapper of a map-reduce task.
</p>
