import tensorflow as tf

session = tf.InteractiveSession() 

x = tf.constant(2)
y = tf.constant(2)
z = x + y

print session.run(z)

