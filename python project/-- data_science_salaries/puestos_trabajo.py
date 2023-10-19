# -*- coding: utf-8 -*-
"""
@author: CPAREJA
"""

from mrjob.job import MRJob
    
class MRTotalesPuestos_Salarios(MRJob):
   
    def mapper(self, _, line):
        linea = line.rstrip().split(";")
        cargo, sueldo, pais = linea[3], linea[4], linea[7]
        yield (cargo, pais), int(sueldo)
              
    def reducer(self, key, values):
        yield key, max(values)
        
if __name__ == '__main__':    
    MRTotalesPuestos_Salarios.run()
