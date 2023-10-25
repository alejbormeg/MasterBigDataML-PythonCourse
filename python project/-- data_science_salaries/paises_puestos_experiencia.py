# -*- coding: utf-8 -*-
"""
@author: Alejandro Borrego Megías
"""

from mrjob.job import MRJob
    
class MRTotalesPuestosSalarios(MRJob):
    def configure_args(self):
        super(MRTotalesPuestosSalarios, self).configure_args()
        self.add_passthru_arg('--country', default='US', help="Indica el código del país")
        self.add_passthru_arg('--job', default='Data Scientist', help="Indica el puesto de trabajo")
        self.add_passthru_arg('--year', default=2020, help="Indica el año")

    def mapper(self, _, line):
        linea = line.rstrip().split(";")
        pais, cargo, anno, experiencia = linea[7], linea[3], linea[0], linea[1]
        if pais == self.options.country and self.options.job in cargo and anno == str(self.options.year):
            yield (pais, cargo, anno, experiencia), 1
              
    def reducer(self, key, values):
        yield key, sum(values)
        
if __name__ == '__main__':    
    MRTotalesPuestosSalarios.run()
