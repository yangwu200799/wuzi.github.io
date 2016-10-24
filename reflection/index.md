---
layout: home
---

<div class="index-content reflection">
    <div class="section">
        <ul class="artical-cate">
            <li><a href="/"><span>Technology Blog</span></a></li>
            <li style="text-align:center"><a href="/life"><span>Life</span></a></li>
            <li class="on" style="text-align:right"><a href="/reflection"><span>Reflection</span></a></li>
        </ul>

        <div class="cate-bar"><span id="cateBar"></span></div>

        <ul class="artical-list">
        {% for post in site.categories.reflection %}
            <li>
                <h2>
                    <a href="{{ post.url }}">{{ post.title }}</a>
                </h2>
                <div class="title-desc">{{ post.description }}</div>
            </li>
        {% endfor %}
        </ul>
    </div>
    <div class="aside">
    </div>
</div>
