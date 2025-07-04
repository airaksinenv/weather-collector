import logging
import azure.functions as func
from shared.main import main as shared_main

def main(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.warning('Timer is past due!')
    
    logging.info('Timer triggered, running shared_main()...')
    shared_main()
    logging.info('shared_main() completed')