document.addEventListener('DOMContentLoaded', () => {
    // State
    const state = {
        offset: 0,
        size: 20,
        loading: false,
        hasMore: true,
        keyword: '',
        baseDate: '',
        selectCategory: null,
        isNew: 0,
        isOld: 0,
        isContinuous: 0,
        isEditing: false,
        selectedIds: new Set(),
        acts: []
    };

    // DOM Elements
    const els = {
        searchInput: document.getElementById('search-input'),
        searchClear: document.getElementById('search-clear'),
        searchCancel: document.getElementById('search-cancel'),
        editModeToggle: document.getElementById('edit-mode-toggle'),
        actsList: document.getElementById('acts-list'),
        loading: document.getElementById('loading'),
        endOfList: document.getElementById('end-of-list'),
        editFooter: document.getElementById('edit-footer'),
        selectedCount: document.getElementById('selected-count'),
        filterBtn: document.getElementById('filter-btn'),
        filterModal: document.getElementById('filter-modal'),
        filterClose: document.getElementById('filter-close'),
        filterApply: document.getElementById('filter-apply'),
        filterBaseDate: document.getElementById('filter-base-date'),
        filterIsNew: document.getElementById('filter-is-new'),
        filterIsOld: document.getElementById('filter-is-old'),
        filterIsContinuous: document.getElementById('filter-is-continuous'),
        categoryList: document.getElementById('category-list'),
        activeFilters: document.getElementById('active-filters')
    };

    // Icon Mapping (Large Category ID -> FontAwesome Class)
    // 仮の定義。必要に応じて修正してください。
    const iconMap = {
        1: 'fa-utensils',          // 食費
        2: 'fa-shopping-basket',   // 日用品
        3: 'fa-tshirt',            // 衣服
        4: 'fa-heartbeat',         // 健康・医療
        5: 'fa-train',             // 交通費
        6: 'fa-wifi',              // 通信費
        7: 'fa-lightbulb',         // 水道・光熱費
        8: 'fa-home',              // 住まい
        9: 'fa-gamepad',           // 趣味・娯楽
        10: 'fa-graduation-cap',   // 教養・教育
        11: 'fa-users',            // 交際費
        12: 'fa-gift',             // 特別な支出
        13: 'fa-wallet',           // 現金・カード
        14: 'fa-piggy-bank',       // 貯金
        15: 'fa-money-bill-wave',  // 給与
        16: 'fa-hand-holding-usd', // その他収入
        17: 'fa-question-circle'   // 未分類
    };

    const getIconClass = (id) => {
        return iconMap[id] || 'fa-circle';
    };

    const getCategoryColor = (id) => {
        // 収入(ID:15, 16等)は青系、支出は赤系などの色分けも可能
        // 今回はシンプルにアイコンのみ
        return 'bg-red-500 text-white'; 
    };

    // --- API Calls ---

    const fetchActs = async (reset = false) => {
        if (state.loading || (!state.hasMore && !reset)) return;
        
        state.loading = true;
        els.loading.classList.remove('hidden');
        
        if (reset) {
            state.offset = 0;
            state.acts = [];
            state.hasMore = true;
            els.actsList.innerHTML = '';
            els.endOfList.classList.add('hidden');
        }

        const params = new URLSearchParams({
            offset: state.offset,
            size: state.size,
            keyword: state.keyword,
        });

        if (state.baseDate) params.append('base_date', state.baseDate);
        if (state.selectCategory) params.append('select_category', state.selectCategory);
        if (state.isNew) params.append('is_new', 1);
        if (state.isOld) params.append('is_old', 1);
        if (state.isContinuous) params.append('is_continuous', 1);

        try {
            const res = await fetch(`/api/acts?${params.toString()}`);
            const data = await res.json();
            
            if (data.error) {
                console.error(data.error);
                alert('データの取得に失敗しました: ' + data.error);
                return;
            }

            if (data.acts.length < state.size) {
                state.hasMore = false;
                els.endOfList.classList.remove('hidden');
            }

            state.acts = [...state.acts, ...data.acts];
            state.offset += data.acts.length;
            
            renderActs(data.acts);

        } catch (e) {
            console.error(e);
            alert('通信エラーが発生しました');
        } finally {
            state.loading = false;
            els.loading.classList.add('hidden');
        }
    };

    const fetchCategories = async () => {
        try {
            const res = await fetch('/api/categories');
            const data = await res.json();
            renderCategoryFilter(data);
        } catch (e) {
            console.error(e);
            els.categoryList.innerHTML = '<div class="p-4 text-red-500">カテゴリの読み込みに失敗しました</div>';
        }
    };

    // --- Rendering ---

    const renderActs = (newActs) => {
        // Group by date
        // 既存のリストの最後の日付グループを取得して、同じ日付ならそこに追加するロジックが必要だが、
        // 簡易的に、今回取得分の中でグルーピングしてappendする。
        // ※厳密にはページ境界で日付がまたがる場合の処理が必要だが、今回はシンプルに実装。
        
        let lastDate = els.actsList.lastElementChild?.dataset?.date;
        let currentGroup = lastDate ? els.actsList.lastElementChild : null;

        newActs.forEach(act => {
            // act.updated_at は ISO 8601 形式 (例: "2025-11-30T12:34:56+09:00")
            // これを "YY/MM/DD" 形式に変換してグルーピングキーとする
            const dateObj = new Date(act.updated_at);
            const yy = String(dateObj.getFullYear()).slice(-2);
            const mm = String(dateObj.getMonth() + 1).padStart(2, '0');
            const dd = String(dateObj.getDate()).padStart(2, '0');
            const dateStr = `${yy}/${mm}/${dd}`; // "25/11/30"

            // 日付表示用にフォーマット変換 (例: 2025年11月30日 (日))
            const days = ['日', '月', '火', '水', '木', '金', '土'];
            const dayOfWeek = days[dateObj.getDay()];
            const displayDate = `${dateObj.getFullYear()}年${dateObj.getMonth() + 1}月${dateObj.getDate()}日 (${dayOfWeek})`;

            if (dateStr !== lastDate) {
                // Create new group
                const group = document.createElement('div');
                group.className = 'group-container';
                group.dataset.date = dateStr;
                
                const header = document.createElement('div');
                header.className = 'bg-gray-100 px-4 py-1 text-xs text-gray-500 font-medium sticky top-[110px] z-10'; // sticky header
                header.textContent = displayDate;
                
                group.appendChild(header);
                els.actsList.appendChild(group);
                currentGroup = group;
                lastDate = dateStr;
            }

            // Create row
            const row = document.createElement('div');
            row.className = 'flex items-center px-4 py-3 bg-white hover:bg-gray-50 transition-colors border-b border-gray-100 last:border-0';
            row.dataset.id = act.id;
            
            // Checkbox (for edit mode)
            const checkboxWrapper = document.createElement('div');
            checkboxWrapper.className = 'edit-checkbox flex-shrink-0';
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.className = 'form-checkbox h-5 w-5 text-blue-600 rounded-full border-gray-300 focus:ring-blue-500';
            checkbox.addEventListener('change', (e) => toggleSelection(act.id, e.target.checked));
            checkboxWrapper.appendChild(checkbox);

            // Icon
            const iconWrapper = document.createElement('div');
            iconWrapper.className = `flex-shrink-0 w-10 h-10 rounded-full flex items-center justify-center mr-3 ${act.is_income ? 'bg-blue-100 text-blue-500' : 'bg-red-100 text-red-500'}`;
            const icon = document.createElement('i');
            icon.className = `fas ${getIconClass(act.large_category_id)}`;
            iconWrapper.appendChild(icon);

            // Content
            const contentWrapper = document.createElement('div');
            contentWrapper.className = 'flex-1 min-w-0';
            const content = document.createElement('div');
            content.className = 'text-sm font-medium text-gray-900 truncate';
            content.textContent = act.content;
            const subContent = document.createElement('div');
            subContent.className = 'text-xs text-gray-500 truncate';
            subContent.textContent = `${act.large_category} / ${act.middle_category}`;
            contentWrapper.appendChild(content);
            contentWrapper.appendChild(subContent);

            // Amount
            const amountWrapper = document.createElement('div');
            amountWrapper.className = `flex-shrink-0 ml-4 text-sm font-bold ${act.is_income ? 'text-blue-600' : 'text-gray-900'}`;
            const amount = Number(act.amount).toLocaleString();
            amountWrapper.textContent = `¥${amount}`;

            row.appendChild(checkboxWrapper);
            row.appendChild(iconWrapper);
            row.appendChild(contentWrapper);
            row.appendChild(amountWrapper);

            // Click handler for row (toggle checkbox in edit mode)
            row.addEventListener('click', (e) => {
                if (state.isEditing) {
                    // Prevent double toggle if clicking directly on checkbox
                    if (e.target !== checkbox) {
                        checkbox.checked = !checkbox.checked;
                        toggleSelection(act.id, checkbox.checked);
                    }
                }
            });

            currentGroup.appendChild(row);
        });
    };

    const renderCategoryFilter = (categories) => {
        els.categoryList.innerHTML = '';
        
        // "指定なし" option
        const allOption = document.createElement('label');
        allOption.className = 'flex items-center p-3 hover:bg-gray-50 cursor-pointer border-b border-gray-100';
        allOption.innerHTML = `
            <input type="radio" name="category" value="" class="form-radio h-4 w-4 text-blue-600" ${state.selectCategory === null ? 'checked' : ''}>
            <span class="ml-3 text-sm text-gray-700">指定なし</span>
        `;
        allOption.querySelector('input').addEventListener('change', () => { state.selectCategory = null; });
        els.categoryList.appendChild(allOption);

        categories.forEach(cat => {
            const label = document.createElement('label');
            label.className = 'flex items-center p-3 hover:bg-gray-50 cursor-pointer border-b border-gray-100 last:border-0';
            label.innerHTML = `
                <input type="radio" name="category" value="${cat.id}" class="form-radio h-4 w-4 text-blue-600" ${state.selectCategory === cat.id ? 'checked' : ''}>
                <span class="ml-3 text-sm text-gray-700 flex items-center">
                    <i class="fas ${getIconClass(cat.id)} w-6 text-center text-gray-400 mr-2"></i>
                    ${cat.name}
                </span>
            `;
            label.querySelector('input').addEventListener('change', () => { state.selectCategory = cat.id; });
            els.categoryList.appendChild(label);
        });
    };

    // --- Event Handlers ---

    // Search
    els.searchInput.addEventListener('focus', () => {
        els.searchCancel.classList.remove('hidden');
    });
    
    els.searchInput.addEventListener('input', (e) => {
        if (e.target.value) {
            els.searchClear.classList.remove('hidden');
        } else {
            els.searchClear.classList.add('hidden');
        }
    });

    els.searchInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            state.keyword = e.target.value;
            fetchActs(true);
            els.searchInput.blur();
        }
    });

    els.searchClear.addEventListener('click', () => {
        els.searchInput.value = '';
        state.keyword = '';
        els.searchClear.classList.add('hidden');
        fetchActs(true);
    });

    els.searchCancel.addEventListener('click', () => {
        els.searchInput.value = '';
        state.keyword = '';
        els.searchClear.classList.add('hidden');
        els.searchCancel.classList.add('hidden');
        fetchActs(true);
    });

    // Edit Mode
    els.editModeToggle.addEventListener('click', () => {
        state.isEditing = !state.isEditing;
        
        if (state.isEditing) {
            document.body.classList.add('editing');
            els.editFooter.classList.remove('translate-y-full');
            els.editModeToggle.innerHTML = '<span class="text-sm font-bold">完了</span>';
        } else {
            document.body.classList.remove('editing');
            els.editFooter.classList.add('translate-y-full');
            els.editModeToggle.innerHTML = '<i class="fas fa-pen"></i>';
            // Clear selection
            state.selectedIds.clear();
            updateSelectionCount();
            document.querySelectorAll('input[type="checkbox"]').forEach(cb => cb.checked = false);
        }
    });

    const toggleSelection = (id, isSelected) => {
        if (isSelected) {
            state.selectedIds.add(id);
        } else {
            state.selectedIds.delete(id);
        }
        updateSelectionCount();
    };

    const updateSelectionCount = () => {
        els.selectedCount.textContent = state.selectedIds.size;
    };

    // Infinite Scroll
    const observer = new IntersectionObserver((entries) => {
        if (entries[0].isIntersecting && state.hasMore && !state.loading) {
            fetchActs();
        }
    }, { threshold: 0.1 });
    
    // Observe a sentinel element at the bottom
    const sentinel = document.createElement('div');
    els.actsList.parentNode.insertBefore(sentinel, els.loading);
    observer.observe(sentinel);


    // Filter Modal
    els.filterBtn.addEventListener('click', () => {
        els.filterModal.classList.remove('hidden');
        // Load categories if not loaded
        if (els.categoryList.children.length <= 1) {
            fetchCategories();
        }
    });

    els.filterClose.addEventListener('click', () => {
        els.filterModal.classList.add('hidden');
    });

    els.filterApply.addEventListener('click', () => {
        state.baseDate = els.filterBaseDate.value;
        state.isNew = els.filterIsNew.checked ? 1 : 0;
        state.isOld = els.filterIsOld.checked ? 1 : 0;
        state.isContinuous = els.filterIsContinuous.checked ? 1 : 0;
        
        // selectCategory is updated by radio change events

        els.filterModal.classList.add('hidden');
        updateActiveFiltersDisplay();
        fetchActs(true);
    });

    const updateActiveFiltersDisplay = () => {
        els.activeFilters.innerHTML = '';
        const filters = [];
        if (state.baseDate) filters.push(state.baseDate);
        if (state.selectCategory) filters.push('カテゴリ指定あり');
        if (state.isNew) filters.push('新着');
        if (state.isOld) filters.push('既読');
        if (state.isContinuous) filters.push('連続');

        filters.forEach(f => {
            const badge = document.createElement('span');
            badge.className = 'px-2 py-1 bg-blue-100 text-blue-600 text-xs rounded-full whitespace-nowrap';
            badge.textContent = f;
            els.activeFilters.appendChild(badge);
        });
    };

    // Initial Load
    fetchActs();
});
