# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from tqdm import tqdm
import sempy.fabric as fabric
import time

def nuke_workspace():
    """
    Deletes all fabric items from a workspace. Think before you delete.
    """      
    items = fabric.list_items()
    nb_id = fabric.get_artifact_id()
    items_to_delete = fabric.list_items().query('Id != @nb_id')['Id']
    workspaceId = fabric.get_notebook_workspace_id()
    workspaceName = fabric.resolve_workspace_name(workspaceId)

    countdown = 10  # Countdown
    print(f"🔴 WARNING: All items from workspace {workspaceName} will be deleted in {countdown} seconds.")
    for remaining in range(countdown, 0, -1):
        print(f"{remaining} seconds remaining... 🚨abort if you are not sure")
        time.sleep(1)

    print(f"{len(items_to_delete)} items will be deleted")
    for item in tqdm(items_to_delete, desc="Deleting items"):
        try:
            fabric.delete_item(item)
        except:
            continue

    remaining_items = fabric.list_items()['Display Name'].to_list()
    print(f"Remaining items: {remaining_items} delete manually")

nuke_workspace()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
