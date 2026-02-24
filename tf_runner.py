import argparse
import subprocess
import sys
import os
from pathlib import Path
from typing import Optional, Generator

def stream_terraform(action: str, module_path: str = "Tasks3_4_5/Modules/Custom_vpc_ec2", var_file: Optional[str] = None) -> Generator[str, None, None]:
    """Run terraform command and yield output lines in real-time.

    Args:
        action: One of 'init', 'plan', or 'apply'.
        module_path: Relative path to the Terraform module.
        var_file: Optional path to a Terraform variable file.

    Yields:
        Lines of output (stdout and stderr).
    """
    tf_dir = Path(__file__).parent / module_path
    if not tf_dir.is_dir():
        yield f"Error: Terraform directory not found: {tf_dir}\n"
        return

    cmd = ["terraform", action]
    if var_file:
        cmd.extend(["-var-file", var_file])
    
    if action in ("plan", "apply"):
        cmd.append("-input=false")
    
    if action in ("apply", "destroy"):
        cmd.append("-auto-approve")

    print(f"Streaming: {' '.join(cmd)} in {tf_dir}")
    
    try:
        # Merge stdout and stderr for simple streaming
        process = subprocess.Popen(
            cmd,
            cwd=tf_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1 # Line buffered
        )

        for line in process.stdout:
            yield line
        
        process.wait()
        if process.returncode != 0:
            yield f"\n[Process exited with code {process.returncode}]\n"
            
    except Exception as e:
        yield f"Exception occurred: {str(e)}\n"

def run_terraform(action: str, module_path: str = "Tasks3_4_5/Modules/Custom_vpc_ec2", var_file: Optional[str] = None):
    """Legacy wrapper for non-streaming calls, still used if needed."""
    from typing import Tuple
    output = []
    success = True
    for line in stream_terraform(action, module_path, var_file):
        output.append(line)
        if "[Process exited with code" in line:
            success = False
    
    return success, "".join(output), ""

def main():
    parser = argparse.ArgumentParser(description="Terraform runner wrapper (Streaming)")
    parser.add_argument("--action", required=True, choices=["init", "plan", "apply", "destroy"], help="Terraform action to perform")
    parser.add_argument("--module", default="Tasks3_4_5/Modules/Custom_vpc_ec2", help="Relative path to Terraform module")
    parser.add_argument("--var-file", help="Path to a .tfvars file")
    args = parser.parse_args()
    
    for line in stream_terraform(args.action, args.module, args.var_file):
        sys.stdout.write(line)
        sys.stdout.flush()

if __name__ == "__main__":
    main()
