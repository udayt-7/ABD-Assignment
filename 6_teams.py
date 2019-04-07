#!/usr/bin/env python
# coding: utf-8

# In[5]:


#get_ipython().run_line_magic('tb', '')
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys  
class MRWordFrequencyCount(MRJob):
	def mapper1(self, _, lines):
		data = lines.split(',')
		players = data[3].strip()
		yield players,None	
	def combiner(self, word, counts):
		
		yield word,None			
		
		
	def reducer1(self, key, counts):

		yield "total teams",key
	def reducer2(self, key,word):
		c =0
		for i in word:
				c+=1	
		
			
		yield key,c
	def steps(self):
		return [ MRStep(mapper=self.mapper1,reducer=self.combiner),MRStep(reducer=self.reducer1),MRStep(reducer=self.reducer2)]



if __name__ == '__main__':
    MRWordFrequencyCount.run()

# In[ ]:




