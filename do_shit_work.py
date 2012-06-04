import os

f = 'storage/'

try:
    os.makedirs(f + '0/')
except:
    pass
users_file = open(f + '0/.users', 'w+')
users_file.write("greg jenny roy")
users_file.close()

greg_friends = open(f + '0/.friends_greg', 'w+')
greg_friends.write('roy jenny')
greg_friends.close()

try:
    os.makedirs(f + '2/')
except:
    pass
jenny_friends = open(f + '2/.friends_jenny', 'w+')
jenny_friends.write('greg')
jenny_friends.close()

try:
    os.makedirs(f + '4/')
except:
    pass
roy_friends = open(f + '4/.friends_roy', 'w+')
roy_friends.write('greg')
roy_friends.close()

server_nums = {0: 'greg', 2: 'jenny', 4: 'roy'}
for num, name in server_nums.iteritems():
    messages = open(f + str(num) + '/.messages_' + name, 'w+')
    messages.close()

    requests = open(f + str(num) + '/.requests_' + name, 'w+')
    requests.close()
