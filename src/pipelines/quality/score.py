class scores:
    ''' CALCULO DOS INDICADORES
    '''
        
    @classmethod
    def tempo_viagem(cls, x:int, *args, **kwargs) -> float:
        ''' Indicador de Tempo de Viagem
        Args:
            x (int): Tempo em minutos da viagem
        Returns:
            Score de tempo viagem.
        '''
        
        x       = abs(x)
        if      x > 80:                 return 0.00
        elif    x > 50  and x <= 80:    return 0.25
        elif    x > 35  and x <= 50:    return 0.50
        elif    x > 20  and x <= 35:    return 0.50
        elif    x > 13  and x <= 20:    return 1.00
        elif    x >  7  and x <= 13:    return 1.25
        elif    x >= 0  and x <= 7:     return 1.50
        else:                           return 0.00

    @classmethod
    def confiabilidade(cls, x:int, *args, **kwargs) -> float:
        ''' Indicador de Confiabilidade
        Args:
            x (int): Tempo em segundos
        Returns:
            _type_: Score de confiança.
        '''
        
        x           = round(x/60, 0)
        # Adianamento e negativo
        if          x < 0:
            x       = abs(x)
            if      x > 12:             return 0.00
            elif    x > 9 and x <= 12:  return 0.25
            elif    x > 6 and x <= 9:   return 0.50
            elif    x > 3 and x <= 6:   return 0.50
            elif    x >= 0 and x <= 3:  return 1.00
            else:                       return 0.00
        # Atraso e Positivo
        elif        x >= 0:
            if      x > 20:             return 0.00
            elif    x > 15 and x <= 20: return 0.25
            elif    x > 10 and x <= 15: return 0.50
            elif    x > 5 and x <= 10:  return 0.50
            elif    x >= 0 and x <= 5:  return 1.00
            else:                       return 0.00
