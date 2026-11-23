### Changed

- **`POST /study/{study_idx}/biosample/{biosample_idx}/link` answers 403 for a biosample
  the caller does not own, and 200 for a link that already exists (#N).** A caller who
  passes the study check but does not own a live biosample now gets 403 instead of 404; one
  who does not own a retired biosample still gets 404. When the pair is already actively
  linked, the route answers 200 with that link, as first created, instead of 409; a retired
  link for the pair is still refused with 409.
