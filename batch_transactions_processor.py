
from redis_manager import RedisManager
from json import loads as json_loads
from colorama import init as colorama_init, Fore, Style

class TxnProcessor():
    """
    TODO 
    implement batch processing with spark
    """

    def __init__(self) -> None:
        self.__redis_manager = RedisManager().get_instance()

    def load_list(self, list_name:str) -> list:
        """
        Loads a whole list
        """
        return [json_loads(x) for x in  self.__redis_manager.lrange(list_name, 0, -1)]
        

if __name__ == "__main__":
    """
    Start the script to see saved transactions
    """
    colorama_init()
    processor = TxnProcessor()
    for i, elem in enumerate(processor.load_list("transactions")):
        print(Fore.GREEN + f"{i} => " + Style.RESET_ALL + str(elem['txn_id']))