import numpy as np

class TurnoutFunction():
    
    def __init__(self, hour_pair:tuple = (8,20), hour_step:float = 0.1, txn_within_step:int = 100) -> None:
        """
        This class models the "turnout" function e.g how many people transacion must take place\n
        within the "txn_within_step". Default configuration express "10 transactions every 6 min"
        - hour_pair: example -> from 8 am to 20 pm  -> (8,20)
        - hour_step: example -> 6 min -> (0.1)
        - txn_within-step: txn per hour_step
        """
        self.__timepoints = np.array(np.arange(hour_pair[0], hour_pair[1], hour_step))
        self.__turnout = np.array([0.0 for num in self.__timepoints])

        for i in range(len(self.__timepoints)):
            # The periodic "SIN(X - 2) + 2" function is evaluated
            # Can be modified to get the most realistic turnout distribution
            self.__turnout[i] = np.sin(self.__timepoints[i]-2) + 2

        # Multiply each for "txn_within_step" to simulate the number of transactions
        # Default is the number of transactions every 0.1 h (6 min)
        self.__turnout = [int(np.floor(txn_within_step*el)) for el in self.__turnout]

    def get_affluence(self) -> list:
        return list(i for i in self.__turnout)
    
    def get_timepoints(self) -> list:
        return self.__timepoints
