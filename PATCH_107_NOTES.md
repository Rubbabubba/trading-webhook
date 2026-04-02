# Patch 107

- Reconciles final submit source inside execute_entry_signal using meta.selected_source when inbound source was downgraded back to worker_scan.
- Prevents early override submissions from falling back into the standard worker path due to downstream default arguments.
