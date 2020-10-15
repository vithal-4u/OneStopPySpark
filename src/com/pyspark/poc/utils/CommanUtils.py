import re

class CommanUtils():

    def __init__(self):
        '''
        constructor
        '''
    #####
    # Below  COMMA_DELIMITER help not split "," value which is have inside a String ex: "Sao Filipe, Fogo Island"
    # 5675,"Sao Filipe Airport","Sao Filipe, Fogo Island","Cape Verde","SFL","GVSF",14.885,-24.48,617,-1,"U","Atlantic/Cape_Verde"
    # 5674,"Praia International Airport","Praia, Santiago Island","Cape Verde","RAI","GVNP",14.9245,-23.4935,230,-1,"U","Atlantic/Cape_Verde"
    #
    #####
    COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')