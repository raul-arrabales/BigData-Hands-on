# Python Hello World


# In[34]:

abs(1 + 2j)


# In[35]:

max(1, 2, 3, 2.56)


# In[36]:

_


# In[37]:

max(1, 2, 3, 5.65, 5)


# In[38]:

round(_)


# In[39]:

number = 3
a, b, c = 1, 2, 3
print(number == c)


# In[40]:

myList = [1, 2.0, 3, 4.0, 5]
print(4 in myList)


# In[42]:

print("Is 4.0 in my List?", 4.0 in myList)


# In[43]:

print("My list is ", len(myList), " positions long.")


# In[44]:

print(myList)


# In[45]:

if b > a:
    print("b is greater than a")
elif a == b: 
    print("a and b are equal")
    print("what a coincidence")
else:
    print("a is grater than b")


# In[46]:

a = b


# In[47]:

if b > a:
    print("b is greater than a")
elif a == b: 
    print("a and b are equal")
    print("what a coincidence")
else:
    print("a is grater than b")


# In[50]:

b = a + 10
if b > a:
    print("b is greater than a")
elif a == b: 
    print("a and b are equal")
print("what a coincidence")


# In[51]:

i = 0
while i < 10: 
    print(i)
    i += 1


# In[52]:

for item in myList:
    print(item)


# In[53]:

for j in range(4):
    print(myList[j])


# In[55]:

for j in range(2):
    print(myList[j])


# In[56]:

def square(x):
    # My square function
    """ Multiple
        lines
        comments
    """
    y = x ** 2
    return y


# In[57]:

square


# In[58]:

square(20)


# In[59]:

def power(x, y=2):
    # Power function
    """ Power of two by default
        Power of y if y is specified
    """
    z = x ** y
    return z


# In[60]:

print("Square: ", power(20))
print("Cube: ", power(20, 3))


# In[61]:

power(20.2)


# In[62]:

power("hola")


# In[63]:

import scipy as sp
import matplotlib.pyplot as plt


# In[65]:

# A one hundred points array from zero to ten
x = sp.linspace(0, 10, 100)


# In[68]:

str(x)


# In[69]:

# y would be the sine of x
y = sp.sin(x)


# In[70]:

y


# In[71]:

# Create a figure
plt.figure()


# In[72]:

# Plot x and y within the figure
plt.plot(x, y)


# In[73]:

# And let's see the graph:
plt.show()


# In[74]:

# Lambda functions:
lanfun = lambda x: x**2


# In[75]:

print(lanfun(2))


# In[76]:

# A function returning a lambda function
def make_incrementor(n): return lambda x: x + n


# In[77]:

f = make_incrementor(2)


# In[78]:

g = make_incrementor(6)


# In[79]:

print(f(42), g(42))


# In[81]:

make_incrementor(22)(33)


# In[86]:

foo = [2, 18, 9, 22, 17, 24, 8, 12, 27]


# In[95]:

f = filter(lambda x: x % 3 == 0, foo)


# In[96]:

DivBy3 = "Mod3 numbers are: "
for item in f:
    DivBy3 += str(item)
    DivBy3 += " "


# In[97]:

DivBy3


# In[98]:

m = map(lambda x: x*2+10, foo)


# In[99]:

for item in m:
    print(item)


# In[100]:

import functools


# In[101]:

r = functools.reduce(lambda x, y: x + y, foo)


# In[102]:

print(r)


# In[103]:

sum(foo) == r



print 'The sum of 1 and 1 is {0}'.format(1+1)

# Import the regular expression library
import re
m = re.search('(?<=abc)def', 'abcdef')
m.group(0)

# Import the datetime library
import datetime
print 'This was last run on: {0}'.format(datetime.datetime.now())




