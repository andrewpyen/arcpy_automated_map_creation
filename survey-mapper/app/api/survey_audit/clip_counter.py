# app/auditing/clip_counter.py
import os
import csv
from datetime import datetime
from typing import Optional, List, Dict

class ClipCounter:
    """
    Collects per-layer clip and merge counts and writes them to CSV.

    Args:
        save_dir (str | None): Directory where audit CSV should be saved.
                               Defaults to <parent_dir>/logs.
        parent_dir (str): Base job output dir, used only if save_dir is not provided.
        logger (logging.Logger | None): Optional logger for status messages.
    """
    def __init__(self, parent_dir: str, logger=None, save_dir: Optional[str] = None):
        self.parent_dir = parent_dir
        self.logger = logger
        # If caller did not supply save_dir, default to <parent_dir>/feature_counts
        self.save_dir = save_dir or os.path.join(parent_dir, "feature_counts")
        os.makedirs(self.save_dir, exist_ok=True)

        self.rows: List[Dict] = []
        self.csv_path: Optional[str] = None

    def open(self, run_label: Optional[str] = None) -> str:
        """Prepare a fresh in-memory table and destination CSV path."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        label = f"_{run_label}" if run_label else ""
        self.csv_path = os.path.join(self.save_dir, f"clip_counts{label}_{ts}.csv")
        self.rows.clear()
        if self.logger:
            self.logger.info(f"ClipAudit opened, writing to {self.csv_path}")
        return self.csv_path

    def add_row(
        self,
        sheet: str,
        source_name: str,
        output_name: str,
        source_count: int,
        selected_count: int,
        clipped_count: int,
        merged_count: Optional[int] = None,
        note: Optional[str] = None,
    ) -> None:
        self.rows.append({
            "sheet": sheet,
            "source_name": source_name,
            "output_name": output_name,
            "source_count": source_count,
            "selected_count": selected_count,
            "clipped_count": clipped_count,
            "merged_count": merged_count if merged_count is not None else "",
            "note": note or "",
        })

    
    def write(self) -> Optional[str]:
        if not self.csv_path:
            return None

        fieldnames = [
            "sheet", "source_name", "output_name",
            "source_count", "selected_count", "clipped_count",
            "merged_count", "note"
        ]
        with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            if self.rows:
                w.writerows(self.rows)

        if self.logger:
            self.logger.info(f"ClipAudit wrote {len(self.rows)} rows to {self.csv_path}")
        return self.csv_path
    
