## 1. Create a temperary file to hold transactions and the last "before" signature in case the program crashes
## 2. Get all transfers transacitoins and save to the temp file, no sorting or filter to ensure optimal speed.
## 3. Sort the transactions using the process_distributor_transactions() and process_distributor_transfers() functions
## 4. Format and save the transfers to the transfers backup directory
## 5. Insert the projects data into the supported project with the last_sig(newest sig)
## 6. Use the controllers aggregate_rewards() function to aggregate the rewards and insert them into the DB
