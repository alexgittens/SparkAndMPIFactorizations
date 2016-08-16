data =  {1: {'kl': 0.27655056846924481, 'times': 6.911053609848023, 'relative_error': 0.81411919227764284, 'rank_approx_error': 0.92557694924684775}, 2: {'kl': 0.20853674978662912, 'times': 4.583123016357422, 'relative_error': 0.81250118021562545, 'rank_approx_error': 0.92557694924684775}, 3: {'kl': 0.18684921183364187, 'times': 5.311400651931763, 'relative_error': 0.81330561703628701, 'rank_approx_error': 0.92557694924684775}, 4: {'kl': 0.11320906817121767, 'times': 24.098127603530884, 'relative_error': 0.81180581390941364, 'rank_approx_error': 0.92557694924684775}, 5: {'kl': 0.10338883684069822, 'times': 12.966964197158813, 'relative_error': 0.81297763669779621, 'rank_approx_error': 0.92557694924684775}}
x = data.keys()
print x
y_r = []
for k,v in data.items():
	y_r.append(v['relative_error'])

import matplotlib.pyplot as plt
def plot(x,y,xlabel, ylabel,title):
	plt.plot(x, y,'b')
	plt.plot(x, y,'bo')
	plt.ylabel(ylabel)
	plt.xlabel(xlabel)
	plt.title(title)
	plt.show()
#plot(x,y_r, 'q','KL divergence btw est. of leverage scores and the exact one','exp 1' )
plot(x,y_r, 'q','relative_error','exp 3' )
"""
data = {100: {'kl': 0.1760473600096184, 'times': 5.295610427856445, 'relative_error': 1.2500360213832489e-09, 'rank_approx_error': 0.92557694924684775}, 70: {'kl': 0.18591344404909302, 'times': 5.203264331817627, 'relative_error': 0.31998861027273873, 'rank_approx_error': 0.92557694924684775}, 200: {'kl': 0.21904899800519412, 'times': 5.237976980209351, 'relative_error': 1.3106084145085216e-11, 'rank_approx_error': 0.92557694924684775}, 10: {'kl': 0.17827596776272731, 'times': 8.259067153930664, 'relative_error': 0.9058258741524956, 'rank_approx_error': 0.92557694924684775}, 50: {'kl': 0.19068923776489571, 'times': 5.72392463684082, 'relative_error': 0.52258617363089366, 'rank_approx_error': 0.92557694924684775}, 20: {'kl': 0.19307870473435323, 'times': 6.0522205352783205, 'relative_error': 0.81233857806223375, 'rank_approx_error': 0.92557694924684775}, 30: {'kl': 0.18961582623633827, 'times': 5.182077217102051, 'relative_error': 0.7187973656454234, 'rank_approx_error': 0.92557694924684775}}
rs = sorted(data.keys())
ys = []
print rs
for k in rs:
	v = data[k]
	ys.append(v['relative_error'])
plot(rs, ys, 'r','relative_error','exp2')
"""
