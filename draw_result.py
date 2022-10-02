import matplotlib.pyplot as plt
import math
losses = []
with open("result.txt", "r") as f:
	idx = 0
	temp = []
	for line in f:
		line_split = line.strip().split("\t")
		if len(line_split) != 2:
			continue
		loss = float(line_split[1])
		if idx%1000 != 0:
			temp.append(loss)
		else:
			avg_loss =0 if (len(temp) == 0) else math.fsum(temp) / len(temp)

			losses.append(avg_loss)
			temp = []
		idx+=1

plt.plot(range(len(losses))[1:], losses[1:])
plt.xlabel('Iteration (x1000)')
plt.ylabel('RMSE loss')
plt.title('My graph')
plt.savefig('loss.png')