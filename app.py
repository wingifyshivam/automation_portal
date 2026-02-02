from flask import Flask, render_template, request, url_for
import subprocess
import json

app = Flask(__name__)

AUTOMATIONS = {
    "Lab Removals": {
        "script": "scripts/new.py",
        "template": "new_params.html",
        "params": ["excel_file_path", "modified_by_function"]
    },
    "CaB Slot Removal": {
        "script": "scripts/cab_slot_removal.py",
        "template": "cab_slot_removal_params.html",
        "params": ["modified_by_function"]
    },
    "Duplicate Recalls": {
        "script": "scripts/duplicate_recalls.py",
        "template": "duplicate_recalls_params.html",
        "params": ["modified_by_function"]
    },
    "Empty Trash": {
        "script": "scripts/empty_trash.py",
        "template": "empty_trash_params.html",
        "params": ["client", "patient_id", "modified_by_function"]
    },
    "Failed to Fetch": {
        "script": "scripts/failed_to_fetch.py",
        "template": "failed_to_fetch_params.html",
        "params": ["patient_id", "modified_by_function"]
    },
    "GDPR": {
        "script": "scripts/GDPR.py",
        "template": "GDPR_params.html",
        "params": ["client", "patient_id", "modified_by_function"]
    },
    "Overlapping graphs": {
        "script": "scripts/overlapping_graphs.py",
        "template": "overlapping_graphs_params.html",
        "params": ["client", "patient_id", "modified_by_function"]
    },
    "Unmerge Patients": {
        "script": "scripts/unmerge.py",
        "template": "unmerge_params.html",
        "params": ["client", "patient_id", "modified_by_function"]
    },
    "New Leaver": {
        "script": "scripts/new_leaver.py",
        "template": "new_leaver_params.html",
        "params": ["username", "modified_by_function"]
    },
    "Remove Card Details": {
        "script": "scripts/remove_card.py",
        "template": "remove_card_params.html",
        "params": ["patient_id", "modified_by_function"]
    },
    "Error 500 when publishing clinical report": {
        "script": "scripts/error_500.py",
        "template": "error_500_params.html",
        "params": ["patient_id", "modified_by_function"]
    },
    "Worklists Backup": {
        "script": "scripts/worklist_backup.py",
        "template": "worklist_backup_params.html",
        "params": ["client", "query", "modified_by_function"]
    },
    "Remove Underscores": {
        "script": "scripts/underscores.py",
        "template": "underscores_params.html",
        "params": ["modified_by_function"]
    },
    "Remove Pink Slots": {
        "script": "scripts/pink_slot_removal.py",
        "template": "pink_slot_removal_params.html",
        "params": ["diary_date", "modified_by_function"]
    }
}

@app.route("/")
def home():
    return render_template("automations.html", automations=AUTOMATIONS)

@app.route("/run/<automation_name>", methods=["GET", "POST"])
def run_automation(automation_name):
    automation = AUTOMATIONS.get(automation_name)
    if request.method == "POST":
        args = [request.form.get(param) for param in automation["params"]]

        result = subprocess.run(
            ["python", automation["script"], *args],
            capture_output=True,
            text=True
        )

        output = result.stdout.strip()
        error = result.stderr.strip()

        data = {}

        if output:
            try:
                data = json.loads(output)
            except json.JSONDecodeError:
                data = {"raw_output": output}
        
        return render_template(
            "results.html",
            output=result.stdout or result.stderr,
            success=(result.returncode == 0),
            implementation_path=data.get("implementation_path"),
            rollback_path=data.get("rollback_path"),
            folder_name = data.get("folder_name"),
            automation_name = automation_name
        )

    return render_template(
        automation["template"],
        automation_name=automation_name
    )

if __name__ == "__main__":
    app.run(debug=True)
