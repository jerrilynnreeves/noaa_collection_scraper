import pytest
from unittest.mock import patch, MagicMock
from noaa_collection_scraper import pipeline_runner

def test_pipeline_runs_all_stages(monkeypatch):
    mock_run = MagicMock(return_value=MagicMock(returncode=0, stdout="ok", stderr=""))
    monkeypatch.setattr("subprocess.run", mock_run)

    pipeline_runner.main()

    # Verify each subprocess was called once in correct order
    called_scripts = [args[0][0][-1] for args in mock_run.call_args_list]
    assert "url_scraper.py" in called_scripts[0]
    assert "metadata_scraper.py" in called_scripts[1]
    assert "osim_meta.py" in called_scripts[2]
    assert "cleanup_artifacts" in called_scripts[3]
    assert mock_run.call_count == 4
