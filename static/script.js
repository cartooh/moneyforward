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
        excludeLargeIds: new Set(),
        excludeMiddleIds: new Set(),
        isEditing: false,
        selectedIds: new Set(),
        acts: [],
        allCategories: [], // Store all categories for bulk update modal
        categoryHistory: JSON.parse(localStorage.getItem('categoryHistory') || '[]') // Load history from local storage
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
        excludeCategoryList: document.getElementById('exclude-category-list'),
        activeFilters: document.getElementById('active-filters'),
        // Bulk Update Modal Elements
        btnBulkCategoryChange: document.getElementById('btn-bulk-category-change'),
        categoryModal: document.getElementById('category-modal'),
        categorySearchInput: document.getElementById('category-search-input'),
        categorySearchClear: document.getElementById('category-search-clear'),
        categoryHistoryList: document.getElementById('category-history-list'),
        largeCategoryList: document.getElementById('large-category-list'),
        middleCategoryList: document.getElementById('middle-category-list'),
        categoryModalClose: document.getElementById('category-modal-close'),
        categoryModalBack: document.getElementById('category-modal-back'),
        toast: document.getElementById('toast'),
        toastMessage: document.getElementById('toast-message')
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
        if (state.selectCategory !== null) params.append('select_category', state.selectCategory);
        if (state.isNew) params.append('is_new', 1);
        if (state.isOld) params.append('is_old', 1);
        if (state.isContinuous) params.append('is_continuous', 1);
        if (state.excludeLargeIds.size > 0) params.append('exclude_large', Array.from(state.excludeLargeIds).join(','));
        if (state.excludeMiddleIds.size > 0) params.append('exclude_middle', Array.from(state.excludeMiddleIds).join(','));

        try {
            const res = await fetch(`/api/acts?${params.toString()}`);
            const data = await res.json();
            
            if (data.error) {
                console.error(data.error);
                alert('データの取得に失敗しました: ' + data.error);
                return;
            }

            if (data.acts.length < state.size && data.total_count <= state.offset + data.fetched_count) {
                // 取得件数が要求より少なく、かつトータル件数に達している場合は終了
                // ※フィルタリングで減っている可能性があるので、total_count との比較は fetched_count を使う
                state.hasMore = false;
                els.endOfList.classList.remove('hidden');
            } else if (data.fetched_count === 0) {
                 // APIから0件しか取れなかった場合も終了
                 state.hasMore = false;
                 els.endOfList.classList.remove('hidden');
            }

            state.acts = [...state.acts, ...data.acts];
            // offsetはAPIから取得した件数分進める（フィルタリング前の件数）
            state.offset += (data.fetched_count !== undefined ? data.fetched_count : data.acts.length);
            
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
            state.allCategories = data; // Save for bulk update
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
        // 1. 絞り込み用 (Radio)
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

        // 2. 除外用 (Checkbox Tree)
        if (!els.excludeCategoryList) return; // 要素がない場合はスキップ
        els.excludeCategoryList.innerHTML = '';

        categories.forEach(cat => {
            const container = document.createElement('div');
            container.className = 'border-b border-gray-100 last:border-0';

            // Large Category Row
            const header = document.createElement('div');
            header.className = 'flex items-center p-3 hover:bg-gray-50';
            
            // Toggle Button (Accordion)
            const toggleBtn = document.createElement('button');
            toggleBtn.className = 'mr-2 text-gray-400 hover:text-gray-600 focus:outline-none';
            toggleBtn.innerHTML = '<i class="fas fa-chevron-right text-xs"></i>';
            
            // Checkbox
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.className = 'form-checkbox h-4 w-4 text-red-600 rounded border-gray-300 focus:ring-red-500';
            checkbox.checked = state.excludeLargeIds.has(cat.id);
            checkbox.addEventListener('change', (e) => {
                if (e.target.checked) {
                    state.excludeLargeIds.add(cat.id);
                } else {
                    state.excludeLargeIds.delete(cat.id);
                }
            });

            // Label
            const label = document.createElement('span');
            label.className = 'ml-3 text-sm text-gray-700 flex-1 flex items-center cursor-pointer';
            label.innerHTML = `<i class="fas ${getIconClass(cat.id)} w-6 text-center text-gray-400 mr-2"></i>${cat.name}`;
            label.addEventListener('click', () => {
                // Toggle accordion on label click
                const subList = container.querySelector('.sub-list');
                const icon = toggleBtn.querySelector('i');
                if (subList.classList.contains('hidden')) {
                    subList.classList.remove('hidden');
                    icon.classList.remove('fa-chevron-right');
                    icon.classList.add('fa-chevron-down');
                } else {
                    subList.classList.add('hidden');
                    icon.classList.remove('fa-chevron-down');
                    icon.classList.add('fa-chevron-right');
                }
            });
            
            // Toggle Button Event
            toggleBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                label.click();
            });

            header.appendChild(toggleBtn);
            header.appendChild(checkbox);
            header.appendChild(label);
            container.appendChild(header);

            // Middle Categories (Sub List)
            const subList = document.createElement('div');
            subList.className = 'sub-list hidden pl-10 pr-3 pb-2 bg-gray-50';
            
            if (cat.middle_categories && cat.middle_categories.length > 0) {
                cat.middle_categories.forEach(mid => {
                    // Wrap input + text in a label so text click toggles checkbox
                    const midItem = document.createElement('label');
                    midItem.className = 'flex items-center py-2 cursor-pointer';

                    const midCheckbox = document.createElement('input');
                    midCheckbox.type = 'checkbox';
                    midCheckbox.className = 'form-checkbox h-3 w-3 text-red-600 rounded border-gray-300 focus:ring-red-500';
                    midCheckbox.checked = state.excludeMiddleIds.has(mid.id);
                    midCheckbox.addEventListener('change', (e) => {
                        if (e.target.checked) {
                            state.excludeMiddleIds.add(mid.id);
                        } else {
                            state.excludeMiddleIds.delete(mid.id);
                        }
                    });

                    const midText = document.createElement('span');
                    midText.className = 'ml-2 text-xs text-gray-600';
                    midText.textContent = mid.name;

                    midItem.appendChild(midCheckbox);
                    midItem.appendChild(midText);
                    subList.appendChild(midItem);
                });
            } else {
                subList.innerHTML = '<div class="text-xs text-gray-400 py-1">中項目なし</div>';
            }

            container.appendChild(subList);
            els.excludeCategoryList.appendChild(container);
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

    // --- Bulk Category Update Logic ---

    const openCategoryModal = () => {
        if (state.selectedIds.size === 0) {
            alert('取引を選択してください');
            return;
        }
        
        // Ensure categories are loaded
        if (state.allCategories.length === 0) {
            fetchCategories().then(() => {
                renderCategoryModalMain();
            });
        } else {
            renderCategoryModalMain();
        }
        
        els.categoryModal.classList.remove('hidden');
        els.categorySearchInput.value = '';
        els.categorySearchInput.focus();
    };

    const closeCategoryModal = () => {
        els.categoryModal.classList.add('hidden');
        // Reset view to main list
        els.largeCategoryList.classList.remove('hidden');
        els.middleCategoryList.classList.add('hidden');
        els.categoryModalBack.classList.add('hidden');
        els.categoryHistoryList.parentElement.classList.remove('hidden'); // Show history container
    };

    const renderCategoryModalMain = () => {
        renderCategoryHistory();
        renderLargeCategories();
        // Reset middle category list
        els.middleCategoryList.innerHTML = '';
    };

    const renderCategoryHistory = () => {
        els.categoryHistoryList.innerHTML = '';
        if (state.categoryHistory.length === 0) {
            els.categoryHistoryList.innerHTML = '<div class="text-xs text-gray-400 p-2">履歴なし</div>';
            return;
        }

        state.categoryHistory.forEach(hist => {
            const btn = document.createElement('button');
            btn.className = 'flex items-center p-2 bg-gray-50 rounded border border-gray-200 mr-2 mb-2 text-sm hover:bg-blue-50 hover:border-blue-200 transition-colors';
            btn.innerHTML = `
                <i class="fas ${getIconClass(hist.large_id)} text-gray-400 mr-2"></i>
                <span class="text-gray-700">${hist.large_name} / ${hist.middle_name}</span>
            `;
            btn.addEventListener('click', () => {
                executeBulkUpdate(hist.large_id, hist.middle_id, hist.large_name, hist.middle_name);
            });
            els.categoryHistoryList.appendChild(btn);
        });
    };

    const renderLargeCategories = () => {
        els.largeCategoryList.innerHTML = '';
        state.allCategories.forEach(cat => {
            const btn = document.createElement('button');
            btn.className = 'w-full flex items-center justify-between p-4 border-b border-gray-100 hover:bg-gray-50 transition-colors text-left';
            btn.innerHTML = `
                <div class="flex items-center">
                    <div class="w-8 h-8 rounded-full bg-gray-100 flex items-center justify-center mr-3">
                        <i class="fas ${getIconClass(cat.id)} text-gray-500"></i>
                    </div>
                    <span class="text-gray-900 font-medium">${cat.name}</span>
                </div>
                <i class="fas fa-chevron-right text-gray-300"></i>
            `;
            btn.addEventListener('click', () => {
                showMiddleCategories(cat);
            });
            els.largeCategoryList.appendChild(btn);
        });
    };

    const showMiddleCategories = (largeCategory) => {
        els.largeCategoryList.classList.add('hidden');
        els.categoryHistoryList.parentElement.classList.add('hidden'); // Hide history
        els.middleCategoryList.classList.remove('hidden');
        els.categoryModalBack.classList.remove('hidden');
        
        els.middleCategoryList.innerHTML = '';
        
        // Header for middle list (optional, maybe just back button is enough)
        
        if (largeCategory.middle_categories && largeCategory.middle_categories.length > 0) {
            largeCategory.middle_categories.forEach(mid => {
                const btn = document.createElement('button');
                btn.className = 'w-full flex items-center p-4 border-b border-gray-100 hover:bg-gray-50 transition-colors text-left';
                btn.innerHTML = `<span class="text-gray-900">${mid.name}</span>`;
                btn.addEventListener('click', () => {
                    executeBulkUpdate(largeCategory.id, mid.id, largeCategory.name, mid.name);
                });
                els.middleCategoryList.appendChild(btn);
            });
        } else {
             els.middleCategoryList.innerHTML = '<div class="p-4 text-gray-500">中項目はありません</div>';
        }
    };

    const filterCategories = (keyword) => {
        if (!keyword) {
            // Reset to initial state
            els.largeCategoryList.classList.remove('hidden');
            els.middleCategoryList.classList.add('hidden');
            els.categoryHistoryList.parentElement.classList.remove('hidden');
            els.categoryModalBack.classList.add('hidden');
            renderLargeCategories();
            return;
        }

        // Search mode: Show flattened list of matches
        els.largeCategoryList.classList.add('hidden');
        els.categoryHistoryList.parentElement.classList.add('hidden');
        els.middleCategoryList.classList.remove('hidden');
        els.categoryModalBack.classList.add('hidden'); // No back button in search mode (clear search to go back)

        els.middleCategoryList.innerHTML = '';
        let hasMatch = false;

        state.allCategories.forEach(large => {
            if (large.middle_categories) {
                large.middle_categories.forEach(mid => {
                    if (mid.name.includes(keyword) || large.name.includes(keyword)) {
                        hasMatch = true;
                        const btn = document.createElement('button');
                        btn.className = 'w-full flex items-center p-4 border-b border-gray-100 hover:bg-gray-50 transition-colors text-left';
                        // Highlight match? For now just simple text
                        btn.innerHTML = `
                            <div class="flex flex-col">
                                <span class="text-gray-900 font-medium">${mid.name}</span>
                                <span class="text-xs text-gray-500">${large.name}</span>
                            </div>
                        `;
                        btn.addEventListener('click', () => {
                            executeBulkUpdate(large.id, mid.id, large.name, mid.name);
                        });
                        els.middleCategoryList.appendChild(btn);
                    }
                });
            }
        });

        if (!hasMatch) {
            els.middleCategoryList.innerHTML = '<div class="p-4 text-gray-500 text-center">一致するカテゴリはありません</div>';
        }
    };

    const executeBulkUpdate = async (largeId, middleId, largeName, middleName) => {
        if (!confirm(`${state.selectedIds.size}件の取引を「${largeName} / ${middleName}」に変更しますか？`)) {
            return;
        }

        // Update History
        const newHistoryItem = { large_id: largeId, middle_id: middleId, large_name: largeName, middle_name: middleName };
        // Remove existing if same
        state.categoryHistory = state.categoryHistory.filter(h => !(h.large_id === largeId && h.middle_id === middleId));
        // Add to front
        state.categoryHistory.unshift(newHistoryItem);
        // Limit to 5
        if (state.categoryHistory.length > 5) state.categoryHistory.pop();
        localStorage.setItem('categoryHistory', JSON.stringify(state.categoryHistory));

        // API Call
        try {
            const res = await fetch('/api/bulk_update_category', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    ids: Array.from(state.selectedIds),
                    large_category_id: largeId,
                    middle_category_id: middleId
                }),
            });
            
            const data = await res.json();
            
            if (data.status === 'success') {
                closeCategoryModal();
                showToast(`${data.updated_count}件のカテゴリを更新しました`);
                
                // Update UI without reload
                // We need to update the DOM elements for the selected IDs
                state.selectedIds.forEach(id => {
                    const row = document.querySelector(`div[data-id="${id}"]`);
                    if (row) {
                        // Update Icon
                        const iconWrapper = row.querySelector('.fa-circle, .fas').parentElement;
                        // Remove old icon class and add new one
                        const icon = iconWrapper.querySelector('i');
                        icon.className = `fas ${getIconClass(largeId)}`;
                        
                        // Update Text
                        const subContent = row.querySelector('.text-xs.text-gray-500');
                        if (subContent) {
                            subContent.textContent = `${largeName} / ${middleName}`;
                        }
                    }
                });

                // Exit edit mode
                els.editModeToggle.click();

            } else {
                alert('更新に失敗しました: ' + (data.message || '不明なエラー'));
            }

        } catch (e) {
            console.error(e);
            alert('通信エラーが発生しました');
        }
    };

    const showToast = (message) => {
        els.toastMessage.textContent = message;
        els.toast.classList.remove('translate-y-full', 'opacity-0');
        setTimeout(() => {
            els.toast.classList.add('translate-y-full', 'opacity-0');
        }, 3000);
    };

    // Event Listeners for Bulk Update
    if (els.btnBulkCategoryChange) {
        els.btnBulkCategoryChange.addEventListener('click', openCategoryModal);
    }

    if (els.categoryModalClose) {
        els.categoryModalClose.addEventListener('click', closeCategoryModal);
    }

    if (els.categoryModalBack) {
        els.categoryModalBack.addEventListener('click', () => {
            els.largeCategoryList.classList.remove('hidden');
            els.middleCategoryList.classList.add('hidden');
            els.categoryModalBack.classList.add('hidden');
            els.categoryHistoryList.parentElement.classList.remove('hidden');
        });
    }

    if (els.categorySearchInput) {
        els.categorySearchInput.addEventListener('input', (e) => {
            const val = e.target.value;
            if (val) {
                els.categorySearchClear.classList.remove('hidden');
            } else {
                els.categorySearchClear.classList.add('hidden');
            }
            filterCategories(val);
        });
    }

    if (els.categorySearchClear) {
        els.categorySearchClear.addEventListener('click', () => {
            els.categorySearchInput.value = '';
            els.categorySearchClear.classList.add('hidden');
            filterCategories('');
            els.categorySearchInput.focus();
        });
    }

});
