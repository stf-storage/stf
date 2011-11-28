[%- MACRO error_msgs(form) BLOCK -%]

[% IF form.has_invalid || form.has_missing %]
<ul>
[% FOREACH name IN form.msgs.keys() %]
<li>[% loc(name) %]</li>
[% END # END FOREACH %]
</ul>
[% END # END IF%]
[% END # END OF MACRO %]

[% MACRO paginate(page) BLOCK %]
<div class="pagination">
[% IF page.previous_page %]
<a href="[% c.uri_for(c.req.path_info, { p => page.previous_page }) %]">Prev</a>
[% ELSE %]
Prev
[% END %]
 | 
[% IF page.next_page %]
<a href="[% c.uri_for(c.req.path_info, { p => page.next_page }) %]">Next</a>
[% ELSE %]
Next
[% END %]
</div>
[%- END # END OF MACRO -%]
