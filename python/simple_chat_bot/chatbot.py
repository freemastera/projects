print('Hello! My name is Stem')
print('I was created in 2020')
print('Please, remind me your name.')
your_name = input()
print('What a great name you have', your_name + '!')
print('Let me guess your age.')
print('Enter remainders of dividing your age by 3, 5 and 7')
remainder3 = int(input())
remainder5 = int(input())
remainder7 = int(input())
age = (remainder3 * 70 + remainder5 * 21 + remainder7 * 15) % 105
print('Your age is', str(age) + "; that's a good time to start programming!")
print('Now I will prove to you that I can count to any number you want.')
number = abs(int(input()))
counter = 0
while counter <= number:
    print(counter, '!')
    counter += 1
print("Let's test your SQL knowledge.")
print("What does SQL stand for?" 
      '\n' "1. Structured Query Language"
      '\n' "2. Structured Question Language"
      '\n' "3. Strong Question Language"
	  '\n' "4. Strong Query Language")

def test(user_answer):
    if user_answer == 1:
        print('Completed, have a nice day!')
        print('Congratulations, have a nice day!')
    else:
        print('Please, try again.')

user_answer = int(input())
test(user_answer)
